//
// Copyright © 2022 Stream.io Inc. All rights reserved.
//

@testable import StreamChat
@testable import StreamChatTestTools
import XCTest

class UserListUpdater_Tests: XCTestCase {
    var webSocketClient: WebSocketClientMock!
    var apiClient: APIClientMock!
    var database: DatabaseContainer!
    
    var listUpdater: UserListUpdater!
    
    override func setUp() {
        super.setUp()
        
        webSocketClient = WebSocketClientMock()
        apiClient = APIClientMock()
        database = DatabaseContainerMock()
        
        listUpdater = UserListUpdater(database: database, apiClient: apiClient)
    }
    
    override func tearDown() {
        apiClient.cleanUp()
        
        AssertAsync {
            Assert.canBeReleased(&listUpdater)
            Assert.canBeReleased(&database)
        }
        
        super.tearDown()
    }
    
    // MARK: - Update
    
    func test_update_makesCorrectAPICall() {
        // Simulate `update` call
        let query = UserListQuery(filter: .equal(.id, to: "Luke"))
        listUpdater.update(userListQuery: query)
        
        let referenceEndpoint: Endpoint<UserListPayload> = .users(query: query)
        XCTAssertEqual(apiClient.request_endpoint, AnyEndpoint(referenceEndpoint))
    }
    
    func test_update_successfullReponseData_areSavedToDB() {
        // Simulate `update` call
        let query = UserListQuery(filter: .equal(.id, to: "Luke"))
        var completionCalled = false
        listUpdater.update(userListQuery: query, completion: { error in
            XCTAssertNil(error)
            completionCalled = true
        })
        
        // Simualte API response with user data
        let dummyUser1 = dummyUser
        let id = dummyUser1.id
        let payload = UserListPayload(users: [dummyUser1])
        apiClient.test_simulateResponse(.success(payload))
        
        // Assert the data is stored in the DB
        var user: ChatUser? {
            database.viewContext.user(id: id)?.asModel()
        }
        
        AssertAsync {
            Assert.willBeTrue(user != nil)
            Assert.willBeTrue(completionCalled)
        }
    }
    
    func test_update_errorResponse_isPropagatedToCompletion() {
        // Simulate `update` call
        let query = UserListQuery(filter: .equal(.id, to: "Luke"))
        var completionCalledError: Error?
        listUpdater.update(userListQuery: query, completion: { completionCalledError = $0 })
        
        // Simualte API response with failure
        let error = TestError()
        apiClient.test_simulateResponse(Result<UserListPayload, Error>.failure(error))
        
        // Assert the completion is called with the error
        AssertAsync.willBeEqual(completionCalledError as? TestError, error)
    }
    
    func test_mergePolicy_takesAffect() throws {
        // Simulate `update` call
        let query = UserListQuery(filter: .equal(.id, to: "Luke"))
        listUpdater.update(userListQuery: query)
        
        // Simulate API response with user data
        let userId = UserId.unique
        let payload = UserListPayload(users: [.dummy(userId: userId)])
        apiClient.test_simulateResponse(.success(payload))
        
        // Assert the data is stored in the DB
        var user: ChatUser? {
            database.viewContext.user(id: userId)?.asModel()
        }
        
        // Assert user is inserted into DB
        AssertAsync.willBeTrue(user != nil)
        
        // Simulate consequent `update` call with new users and `.merge` policy
        // We don't pass the `policy` argument since we expect it's `merge` by default
        listUpdater.update(userListQuery: query)
        
        // Simulate API response with user data
        let newUserId = UserId.unique
        let newPayload = UserListPayload(users: [.dummy(userId: newUserId)])
        apiClient.test_simulateResponse(.success(newPayload))
        
        // Assert the data is stored in the DB
        var newUser: ChatUser? {
            database.viewContext.user(id: newUserId)?.asModel()
        }
        
        // Assert new user is inserted into DB
        AssertAsync.willBeTrue(newUser != nil)
        
        let userIds = [user!, newUser!].map(\.id)
        
        // Assert both users are linked to the same query now
        try database.writeSynchronously { session in
            do {
                let dto = try session.saveQuery(query: query)
                XCTAssertEqual(dto!.users.count, 2)
                XCTAssertEqual(
                    dto!.users.map(\.id).sorted(),
                    userIds.sorted()
                )
            } catch {
                XCTFail("Error trying to get query: \(error)")
            }
        }
    }
    
    func test_removePolicy_takesAffect() throws {
        // Create query
        let filterHash = String.unique
        var query = UserListQuery(filter: .equal(.id, to: "Luke"))
        query.filter?.explicitHash = filterHash
        // Simulate `update` call
        // This call doesn't need `policy` argument specified since
        // it's the first call for this query, hence there's no data to `replace` or `merge` to
        listUpdater.update(userListQuery: query)
        
        // Simulate API response with user data
        let userId = UserId.unique
        let payload = UserListPayload(users: [.dummy(userId: userId)])
        apiClient.test_simulateResponse(.success(payload))
        
        // Assert the data is stored in the DB
        var user: ChatUser? {
            database.viewContext.user(id: userId)?.asModel()
        }
        
        // Assert user is inserted into DB
        AssertAsync.willBeTrue(user != nil)
        
        // Simulate consequent `update` call with new users and `.replace` policy
        listUpdater.update(userListQuery: query, policy: .replace)
        
        // Simulate API response with user data
        let newUserId = UserId.unique
        let newPayload = UserListPayload(users: [.dummy(userId: newUserId)])
        apiClient.test_simulateResponse(.success(newPayload))
        
        // Assert the data is stored in the DB
        var newUser: ChatUser? {
            database.viewContext.user(id: newUserId)?.asModel()
        }
        
        // Assert new user is inserted into DB
        AssertAsync.willBeTrue(newUser != nil)
        
        // Assert first user is not linked to the query anymore
        var queryDTO: UserListQueryDTO? {
            database.viewContext.userListQuery(filterHash: filterHash)
        }
        
        // Assert only 1 user is linked to query
        XCTAssertEqual(queryDTO!.users.count, 1)
        // Assert new user is linked to query
        XCTAssertEqual(queryDTO!.users.map(\.id), [newUser!].map(\.id))
    }
    
    func test_updateCompletion_calledAfterDBWriteCompletes() {
        let dummyUserId = UserId.unique
        
        // Simulate `update` call
        let query = UserListQuery(filter: .equal(.id, to: "Luke"))
        var completionCalled = false
        listUpdater.update(userListQuery: query, completion: { _ in
            // At this point, DB write should have completed
            
            // Assert the data is stored in the DB
            // We call this block in `main` queue since we need to access `viewContext`
            DispatchQueue.main.sync {
                let user: ChatUser? = self.database.viewContext.user(id: dummyUserId)?.asModel()
            
                XCTAssert(user != nil)
                
                completionCalled = true
            }
        })
        
        // Simulate API response with user data
        let user = dummyUser(id: dummyUserId)
        let payload = UserListPayload(users: [user])
        apiClient.test_simulateResponse(.success(payload))
        
        AssertAsync.willBeTrue(completionCalled)
    }
}
