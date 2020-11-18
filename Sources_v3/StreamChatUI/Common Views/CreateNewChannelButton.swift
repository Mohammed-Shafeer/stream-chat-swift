//
// Copyright © 2020 Stream.io Inc. All rights reserved.
//

import StreamChat
import UIKit

open class CreateNewChannelButton: UIButton {
    // MARK: - Overrides
    
    open var defaultIntrinsicContentSize: CGSize?
    override open var intrinsicContentSize: CGSize {
        defaultIntrinsicContentSize ?? super.intrinsicContentSize
    }
    
    // MARK: - Init
    
    override public init(frame: CGRect) {
        super.init(frame: frame)
        applyDefaultAppearance()
    }
    
    public required init?(coder: NSCoder) {
        super.init(coder: coder)
        applyDefaultAppearance()
    }
}

// MARK: - AppearanceSetting

extension CreateNewChannelButton: AppearanceSetting {
    public static func initialAppearanceSetup(_ button: CreateNewChannelButton) {
        button.defaultIntrinsicContentSize = .init(width: 44, height: 44)
        button.setImage(UIImage(named: "icn_new_chat", in: Bundle(for: Self.self), compatibleWith: nil), for: .normal)
    }
}
