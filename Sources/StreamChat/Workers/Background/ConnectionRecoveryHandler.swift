//
// Copyright © 2021 Stream.io Inc. All rights reserved.
//

import CoreData
import Foundation

/// The type that descibes chat component that might need recovery when client reconnects.
protocol ChatRecoverableComponent: AnyObject {
    typealias LocalSyncedCIDs = Set<ChannelId>
    typealias LocalWatchedCIDs = Set<ChannelId>
    
    /// Says if the component needs recovery.
    var requiresRecovery: Bool { get }
    
    /// Recovers a component giving it information about already synced and watched channels.
    ///
    /// The completion should return set of channel IDs related to this component that were recovered and watched.
    func recover(
        syncedCIDs: LocalSyncedCIDs,
        watchedCIDs: LocalWatchedCIDs,
        completion: @escaping (Result<LocalWatchedCIDs, Error>) -> Void
    )
}

/// The type that keeps track of active chat components and asks them to reconnect when it's needed
protocol ConnectionRecoveryHandler: ConnectionStateDelegate {
    /// The array of registered channel list components.
    var registeredChannelLists: [ChatRecoverableComponent] { get }
    
    /// The array of registered channels components.
    var registeredChannels: [ChatRecoverableComponent] { get }

    /// Registers channel list component as one that might need recovery on reconnect.
    func register(channelList: ChatRecoverableComponent)
    
    /// Registers channel component as one that might need recovery on reconnect.
    func register(channel: ChatRecoverableComponent)
}

extension ConnectionRecoveryHandler {
    /// Returns registered channel list components that require recovery.
    var channelListsToRecover: [ChatRecoverableComponent] {
        registeredChannelLists.filter(\.requiresRecovery)
    }
    
    /// Returns registered channel components that require recovery.
    var channelsToRecover: [ChatRecoverableComponent] {
        registeredChannels.filter(\.requiresRecovery)
    }
}

/// The type is designed to obtain missing events that happened in watched channels while user
/// was not connected to the web-socket.
///
/// The object listens for `ConnectionStatusUpdated` events
/// and remembers the `CurrentUserDTO.lastReceivedEventDate` when status becomes `connecting`.
///
/// When the status becomes `connected` the `/sync` endpoint is called
/// with `lastReceivedEventDate` and `cids` of watched channels.
///
/// We remember `lastReceivedEventDate` when state becomes `connecting` to catch the last event date
/// before the `HealthCheck` override the `lastReceivedEventDate` with the recent date.
///
final class DefaultConnectionRecoveryHandler {
    // MARK: - Properties
    
    private let database: DatabaseContainer
    private let apiClient: APIClient
    private let webSocketClient: WebSocketClient
    private let eventNotificationCenter: EventNotificationCenter
    private let backgroundTaskScheduler: BackgroundTaskScheduler?
    private let internetConnection: InternetConnection
    private let reconnectionTimerType: Timer.Type
    private var reconnectionStrategy: RetryStrategy
    private var reconnectionTimer: TimerControl?
    private let keepConnectionAliveInBackground: Bool
    
    private let componentsAccessQueue = DispatchQueue(label: "co.getStream.ConnectionRecoveryUpdater")
    private var channelLists: [Weak<ChatRecoverableComponent>] = []
    private var channels: [Weak<ChatRecoverableComponent>] = []
    
    // MARK: - Init
    
    init(
        database: DatabaseContainer,
        apiClient: APIClient,
        webSocketClient: WebSocketClient,
        eventNotificationCenter: EventNotificationCenter,
        backgroundTaskScheduler: BackgroundTaskScheduler?,
        internetConnection: InternetConnection,
        reconnectionStrategy: RetryStrategy,
        reconnectionTimerType: Timer.Type,
        keepConnectionAliveInBackground: Bool
    ) {
        self.database = database
        self.apiClient = apiClient
        self.webSocketClient = webSocketClient
        self.eventNotificationCenter = eventNotificationCenter
        self.backgroundTaskScheduler = backgroundTaskScheduler
        self.internetConnection = internetConnection
        self.reconnectionStrategy = reconnectionStrategy
        self.reconnectionTimerType = reconnectionTimerType
        self.keepConnectionAliveInBackground = keepConnectionAliveInBackground

        subscribeOnNotifications()
    }
    
    deinit {
        unsubscribeFromNotifications()
        cancelReconnectionTimer()
    }
}

// MARK: - ConnectionRecoveryHandler

extension DefaultConnectionRecoveryHandler: ConnectionRecoveryHandler {
    var registeredChannelLists: [ChatRecoverableComponent] {
        channelLists.compactMap(\.value)
    }
    
    var registeredChannels: [ChatRecoverableComponent] {
        channels.compactMap(\.value)
    }
    
    func register(channelList: ChatRecoverableComponent) {
        componentsAccessQueue.sync {
            channelLists.removeAll(where: { $0.value == nil || $0.value === channelList })
            channelLists.append(.init(value: channelList))
        }
    }
    
    func register(channel: ChatRecoverableComponent) {
        componentsAccessQueue.sync {
            channels.removeAll(where: { $0.value == nil || $0.value === channel })
            channels.append(.init(value: channel))
        }
    }
    
    func webSocketClient(_ client: WebSocketClient, didUpdateConnectionState state: WebSocketConnectionState) {
        log.debug("Connection state: \(state)", subsystems: .webSocket)
        
        switch state {
        case .connecting:
            cancelReconnectionTimer()
            
        case .connected:
            reconnectionStrategy.resetConsecutiveFailures()
            
            syncLocalStateWithRemote { [weak self] in
                if let error = $0 {
                    log.info("❌ Local state is NOT synced with remote: \(error.localizedDescription)")
                    
                    self?.disconnectIfNeeded()
                } else {
                    log.info("✅ Local state is synced with remote")
                }
            }
        case .disconnected:
            scheduleReconnectionTimerIfNeeded()
            
        case .initialized, .waitingForConnectionId, .disconnecting:
            break
        }
    }
}

// MARK: - Subscriptions

private extension DefaultConnectionRecoveryHandler {
    func subscribeOnNotifications() {
        backgroundTaskScheduler?.startListeningForAppStateUpdates(
            onEnteringBackground: { [weak self] in self?.appDidEnterBackground() },
            onEnteringForeground: { [weak self] in self?.appDidBecomeActive() }
        )
        
        internetConnection.notificationCenter.addObserver(
            self,
            selector: #selector(internetConnectionAvailabilityDidChange(_:)),
            name: .internetConnectionAvailabilityDidChange,
            object: nil
        )
    }
    
    func unsubscribeFromNotifications() {
        backgroundTaskScheduler?.stopListeningForAppStateUpdates()
        
        internetConnection.notificationCenter.removeObserver(
            self,
            name: .internetConnectionStatusDidChange,
            object: nil
        )
    }
}

// MARK: - Event handlers

extension DefaultConnectionRecoveryHandler {
    private func appDidBecomeActive() {
        log.debug("App -> ✅", subsystems: .webSocket)
        
        backgroundTaskScheduler?.endTask()
        
        reconnectIfNeeded()
    }
    
    private func appDidEnterBackground() {
        log.debug("App -> 💤", subsystems: .webSocket)
        
        guard canBeDisconnected else {
            // Client is not trying to connect nor connected
            return
        }
        
        guard keepConnectionAliveInBackground else {
            // We immediately disconnect
            disconnectIfNeeded()
            return
        }
        
        guard let scheduler = backgroundTaskScheduler else { return }
                
        let succeed = scheduler.beginTask { [weak self] in
            log.debug("Background task -> ❌", subsystems: .webSocket)
            
            self?.disconnectIfNeeded()
        }
        
        if succeed {
            log.debug("Background task -> ✅", subsystems: .webSocket)
        } else {
            // Can't initiate a background task, close the connection
            disconnectIfNeeded()
        }
    }
    
    @objc private func internetConnectionAvailabilityDidChange(_ notification: Notification) {
        guard let isAvailable = notification.internetConnectionStatus?.isAvailable else { return }
        
        log.debug("Internet -> \(isAvailable ? "✅" : "❌")", subsystems: .webSocket)
        
        if isAvailable {
            reconnectIfNeeded()
        } else {
            disconnectIfNeeded()
        }
    }
}

// MARK: - Disconnection

private extension DefaultConnectionRecoveryHandler {
    func disconnectIfNeeded() {
        guard canBeDisconnected else { return }
        
        webSocketClient.disconnect(source: .systemInitiated)
    }
    
    var canBeDisconnected: Bool {
        let state = webSocketClient.connectionState
        
        switch state {
        case .connecting, .waitingForConnectionId, .connected:
            log.debug("Will disconnect automatically from \(state) state", subsystems: .webSocket)
            
            return true
        default:
            log.debug("Disconnect is not needed in \(state) state", subsystems: .webSocket)
            
            return false
        }
    }
}

// MARK: - Reconnection

private extension DefaultConnectionRecoveryHandler {
    func reconnectIfNeeded() {
        guard canReconnectAutomatically else { return }
                
        webSocketClient.connect()
    }
    
    var canReconnectAutomatically: Bool {
        guard webSocketClient.connectionState.isAutomaticReconnectionEnabled else {
            log.debug("Reconnection is not required (\(webSocketClient.connectionState))", subsystems: .webSocket)
            return false
        }
        
        guard internetConnection.status.isAvailable else {
            log.debug("Reconnection is not possible (internet ❌)", subsystems: .webSocket)
            return false
        }
        
        guard backgroundTaskScheduler?.isAppActive ?? true else {
            log.debug("Reconnection is not possible (app 💤)", subsystems: .webSocket)
            return false
        }
        
        log.debug("Will reconnect automatically", subsystems: .webSocket)
        
        return true
    }
}

// MARK: - Syncing local data with remote

private extension DefaultConnectionRecoveryHandler {
    func syncLocalStateWithRemote(completion: @escaping (Error?) -> Void) {
        // 1. Load last sync timestamp
        loadLastSyncDate { [weak self] in
            guard let lastSyncedAt = $0 else {
                // That's the first session of the current user. Bump `lastSyncedAt` with current time and return.
                self?.bumpLastSyncDate(.init()) { completion(nil) }
                return
            }
            
            // 2. Load locally existed channel identifiers
            self?.loadLocalChannels { cidsToSync in
                // 3. Fetch missing events and save to database
                self?.fetchAndSaveMissingEvents(for: cidsToSync, since: lastSyncedAt) {
                    guard case .success(let (syncedCIDs, mostRecentEventDate)) = $0 else {
                        completion($0.error)
                        return
                    }
                    
                    // 4. Recover active channels
                    self?.recover(
                        channels: self?.channelsToRecover ?? [],
                        syncedCIDs: syncedCIDs
                    ) {
                        guard case let .success(watchedCIDs) = $0 else {
                            completion($0.error)
                            return
                        }
                        
                        // 5. Recover active channel lists
                        self?.recover(
                            channelLists: self?.channelListsToRecover ?? [],
                            syncedCIDs: syncedCIDs,
                            watchedCIDs: watchedCIDs
                        ) {
                            guard $0 == nil else {
                                completion($0)
                                return
                            }
                            
                            // 6. Update last sync date since all missing events were applied
                            self?.bumpLastSyncDate(mostRecentEventDate) {
                                completion(nil)
                            }
                        }
                    }
                }
            }
        }
    }
    
    func fetchAndSaveMissingEvents(
        for cids: Set<ChannelId>,
        since lastSyncedAt: Date,
        completion: @escaping (Result<(Set<ChannelId>, Date), Error>) -> Void
    ) {
        guard !cids.isEmpty else {
            completion(.success((cids, lastSyncedAt)))
            return
        }
        
        apiClient.request(
            endpoint: .missingEvents(since: lastSyncedAt, cids: .init(cids))
        ) { [weak self] in
            switch $0 {
            case let .success(payload):
                self?.eventNotificationCenter.process(
                    payload.eventPayloads.asEvents(),
                    postNotifications: false
                ) {
                    let mostRecentEventTimestamp = payload.eventPayloads.last?.createdAt ?? lastSyncedAt
                    
                    completion(.success((cids, mostRecentEventTimestamp)))
                }
            case let .failure(error):
                guard error.isTooManyMissingEventsToSyncError else {
                    log.error("Fail to get missing events: \(error)")
                    completion(.failure(error))
                    return
                }
                
                log.info(
                    """
                    Backend couldn't handle replaying missing events - there was too many (>1000) events to replay. \
                    Cleaning local channels data and refetching it from scratch
                    """
                )
                
                completion(.success(([], Date())))
            }
        }
    }
    
    func recover(
        channels: [ChatRecoverableComponent],
        syncedCIDs: ChatRecoverableComponent.LocalSyncedCIDs,
        completion: @escaping (Result<ChatRecoverableComponent.LocalWatchedCIDs, Error>) -> Void
    ) {
        let group = DispatchGroup()
        let semaphore = DispatchSemaphore(value: 1)
        
        var watchedCIDs = ChatRecoverableComponent.LocalWatchedCIDs()
        var errors = [Error]()
        
        for channel in channels {
            group.enter()
            
            channel.recover(syncedCIDs: syncedCIDs, watchedCIDs: []) {
                semaphore.wait()
                switch $0 {
                case let .success(newWatchedCIDs):
                    watchedCIDs.formUnion(newWatchedCIDs)
                case let .failure(error):
                    errors.append(error)
                }
                semaphore.signal()
                
                group.leave()
            }
        }
        
        group.notify(queue: .main) {
            if let error = errors.first {
                completion(.failure(error))
            } else {
                completion(.success(watchedCIDs))
            }
        }
    }
    
    func recover(
        channelLists: [ChatRecoverableComponent],
        syncedCIDs: ChatRecoverableComponent.LocalSyncedCIDs,
        watchedCIDs: ChatRecoverableComponent.LocalWatchedCIDs,
        completion: @escaping (Error?) -> Void
    ) {
        guard let channelList = channelLists.first else {
            completion(nil)
            return
        }
        
        channelList.recover(syncedCIDs: syncedCIDs, watchedCIDs: watchedCIDs) { [weak self] in
            switch $0 {
            case let .success(newWatchedCIDs):
                self?.recover(
                    channelLists: .init(channelLists.dropFirst()),
                    syncedCIDs: syncedCIDs,
                    watchedCIDs: watchedCIDs.union(newWatchedCIDs),
                    completion: completion
                )
            case let .failure(error):
                completion(error)
            }
        }
    }
    
    func bumpLastSyncDate(_ lastSyncedAt: Date, completion: @escaping () -> Void) {
        database.write({ session in
            session.currentUser?.lastSyncedAt = lastSyncedAt
        }, completion: { _ in
            completion()
        })
    }
    
    func loadLocalChannels(completion: @escaping (Set<ChannelId>) -> Void) {
        database.write { session in
            let cids = Set(
                session
                    .loadAllChannelListQueries()
                    .flatMap(\.channels)
                    .compactMap { try? ChannelId(cid: $0.cid) }
            )
            
            completion(cids)
        }
    }
    
    func loadLastSyncDate(completion: @escaping (Date?) -> Void) {
        database.write { session in
            let lastSyncedAt = session.currentUser?.lastSyncedAt
            
            completion(lastSyncedAt)
        }
    }
}

// MARK: - Reconnection Timer

private extension DefaultConnectionRecoveryHandler {
    func scheduleReconnectionTimerIfNeeded() {
        guard canReconnectAutomatically else { return }
        
        scheduleReconnectionTimer()
    }
    
    func scheduleReconnectionTimer() {
        let delay = reconnectionStrategy.getDelayAfterTheFailure()
        
        log.debug("Timer ⏳ \(delay) sec", subsystems: .webSocket)
        
        reconnectionTimer = reconnectionTimerType.schedule(
            timeInterval: delay,
            queue: .main,
            onFire: { [weak self] in
                log.debug("Timer 🔥", subsystems: .webSocket)
                
                self?.reconnectIfNeeded()
            }
        )
    }
    
    func cancelReconnectionTimer() {
        guard reconnectionTimer != nil else { return }
        
        log.debug("Timer ❌", subsystems: .webSocket)
        
        reconnectionTimer?.cancel()
        reconnectionTimer = nil
    }
}

extension Error {
    /// Backend responds with 400 if there was more than 1000 events to replay
    var isTooManyMissingEventsToSyncError: Bool {
        isBackendErrorWith400StatusCode
    }
}
