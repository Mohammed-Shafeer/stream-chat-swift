//
// Copyright © 2021 Stream.io Inc. All rights reserved.
//

import CoreData

/// After creating new user it's not observed cause it's not linked to any UserListQuery.
/// The only job of `NewUserQueryUpdater` is to find whether new user belongs to any of the exsisting queries
/// and link it to the user if so.
///     1. This worker observers DB for the insertations of the new `UserDTO`s without any linked queries.
///     2. When new user is found, all exsisting queries are fetched from DB and we modify exsiting queries filters so
///     in response for `update(userListQuery` request new user will be returned if it is part of the original query filter.
///     3. After sending `update(userListQuery` for all queries `UserListUpdater` does the job of linking
///     corresponding queries to the user.
final class NewUserQueryUpdater: Worker {
    private let environment: Environment
        
    private lazy var userListUpdater: UserListUpdater = self.environment
        .createUserListUpdater(
            database,
            apiClient
        )
    
    private lazy var usersObserver: ListDatabaseObserver = .init(
        context: self.database.backgroundReadOnlyContext,
        fetchRequest: UserDTO.userWithoutQueryFetchRequest
    )
    
    private var queries: [UserListQueryDTO] {
        do {
            let queries = try database.backgroundReadOnlyContext
                .fetch(UserListQueryDTO.observedQueries())
            return queries
        } catch {
            log.error("Internal error: Failed to fetch [UserListQueryDTO]: \(error)")
        }
        return []
    }
    
    init(database: DatabaseContainer, apiClient: APIClient, env: Environment) {
        environment = env
        super.init(database: database, apiClient: apiClient)
        
        startObserving()
    }
    
    override convenience init(database: DatabaseContainer, apiClient: APIClient) {
        self.init(database: database, apiClient: apiClient, env: .init())
    }
    
    private func startObserving() {
        // We have to initialize the lazy variables synchronously
        _ = userListUpdater
        _ = usersObserver
        
        // But the observing can be started on a background queue
        DispatchQueue.global().async { [weak self] in
            do {
                self?.usersObserver.onChange = { changes in
                    self?.handle(changes: changes)
                }
                try self?.usersObserver.startObserving()
                self?.usersObserver.items.forEach { self?.updateUserListQuery(for: $0) }
            } catch {
                log.error("Error starting NewUserQueryUpdater observer: \(error)")
            }
        }
    }
    
    private func handle(changes: [ListChange<UserDTO>]) {
        // Observe `UserDTO` insertations
        changes.forEach { change in
            switch change {
            case let .insert(userDTO, _):
                updateUserListQuery(for: userDTO)
            default: return
            }
        }
    }
    
    private func updateUserListQuery(for userDTO: UserDTO) {
        database.backgroundReadOnlyContext.perform { [weak self] in
            let userID = userDTO.id
            let queryDTOs = self?.queries ?? []
            
            queryDTOs.forEach { dto in
                let queryHash = dto.filterHash
               
                do {
                    let queryMatchingUser = try dto.asUserListQueryWithUpdatedFilter(filterToAdd: .equal("id", to: userID))
                    
                    self?.userListUpdater.fetch(userListQuery: queryMatchingUser) {
                        switch $0 {
                        case let .success(userListPayload):
                            self?.database.write { session in
                                guard
                                    let queryDTO = session.userListQuery(filterHash: queryHash),
                                    let userDTO = session.user(id: userID)
                                else { return }
                                
                                if userListPayload.users.contains(where: { $0.id == userID }) {
                                    queryDTO.users.insert(userDTO)
                                } else {
                                    queryDTO.users.remove(userDTO)
                                }
                            }
                        case let .failure(error):
                            log.error("Failed to check if user with id \(userID) matches query: \(queryHash): \(error)")
                        }
                    }
                } catch {
                    log.error("Failed to decode model from user list query: \(queryHash)")
                }
            }
        }
    }
}

extension NewUserQueryUpdater {
    struct Environment {
        var createUserListUpdater: (
            _ database: DatabaseContainer,
            _ apiClient: APIClient
        ) -> UserListUpdater = UserListUpdater.init
    }
}

private extension UserListQueryDTO {
    func asUserListQueryWithUpdatedFilter(
        filterToAdd filter: Filter<UserListFilterScope>
    ) throws -> UserListQuery {
        let originalFilter = try JSONDecoder.default.decode(Filter<UserListFilterScope>.self, from: filterJSONData)
        
        return UserListQuery(
            filter: .and([originalFilter, filter])
        )
    }
}
