//
// Copyright © 2021 Stream.io Inc. All rights reserved.
//

import SwiftUI

public struct ReactionsOverlayView<Factory: ViewFactory>: View {
    @StateObject var viewModel: ReactionsOverlayViewModel
    
    var factory: Factory
    var currentSnapshot: UIImage
    var messageDisplayInfo: MessageDisplayInfo
    var onBackgroundTap: () -> Void
    
    public init(
        factory: Factory,
        currentSnapshot: UIImage,
        messageDisplayInfo: MessageDisplayInfo,
        onBackgroundTap: @escaping () -> Void
    ) {
        _viewModel = StateObject(
            wrappedValue: ViewModelsFactory.makeReactionsOverlayViewModel(
                message: messageDisplayInfo.message
            )
        )
        self.factory = factory
        self.currentSnapshot = currentSnapshot
        self.messageDisplayInfo = messageDisplayInfo
        self.onBackgroundTap = onBackgroundTap
    }
    
    public var body: some View {
        ZStack(alignment: .topLeading) {
            Image(uiImage: currentSnapshot)
                .blur(radius: 8)
                .transition(.opacity)
                .onTapGesture {
                    withAnimation {
                        onBackgroundTap()
                    }
                }
                .edgesIgnoringSafeArea(.all)
            
            VStack {
                MessageView(
                    factory: factory,
                    message: messageDisplayInfo.message,
                    contentWidth: messageDisplayInfo.contentWidth,
                    isFirst: messageDisplayInfo.isFirst
                )
                .offset(
                    x: messageDisplayInfo.frame.origin.x,
                    y: originY
                )
                .frame(
                    width: messageDisplayInfo.frame.width,
                    height: messageDisplayInfo.frame.height
                )
                .overlay(
                    ReactionsOverlayContainer(
                        message: viewModel.message,
                        contentRect: messageDisplayInfo.frame,
                        onReactionTap: { reaction in
                            viewModel.reactionTapped(reaction)
                            onBackgroundTap()
                        }
                    )
                    .id(viewModel.message.reactionScoresId)
                    .offset(
                        x: messageDisplayInfo.frame.origin.x,
                        y: originY - 24
                    )
                )
            }
        }
        .edgesIgnoringSafeArea(.all)
    }
    
    private var originY: CGFloat {
        var originY = messageDisplayInfo.frame.origin.y
        let screenHeight = UIScreen.main.bounds.size.height
        let minOrigin: CGFloat = 100
        let maxOrigin: CGFloat = screenHeight - messageDisplayInfo.frame.height - minOrigin
        if originY < minOrigin {
            originY = minOrigin
        } else if originY > maxOrigin {
            originY = maxOrigin
        }
        
        return originY
    }
}
