//
// Copyright © 2022 Stream.io Inc. All rights reserved.
//

import Foundation

enum AttachmentCodingKeys: String, CodingKey, CaseIterable {
    case title
    case type
    case image
    case url
    case name
    case text
    case author = "author_name"
    case ogURL = "og_scrape_url"
    case thumbURL = "thumb_url"
    case fallback
    case imageURL = "image_url"
    case assetURL = "asset_url"
    case titleLink = "title_link"
    case actions
}

/// A local state of the attachment. Applies only for attachments linked to the new messages sent from current device.
public enum LocalAttachmentState: Hashable {
    /// The current state is unknown
    case unknown
    /// The attachment is waiting to be uploaded.
    case pendingUpload
    /// The attachment is currently being uploaded. The progress in [0, 1] range.
    case uploading(progress: Double)
    /// Uploading of the message failed. The system will not trying to upload this attachment anymore.
    case uploadingFailed
    /// The attachment is successfully uploaded.
    case uploaded
}

/// An attachment action, e.g. send, shuffle.
public struct AttachmentAction: Codable, Hashable {
    /// A name.
    public let name: String
    /// A value of an action.
    public let value: String
    /// A style, e.g. primary button.
    public let style: ActionStyle
    /// A type, e.g. button.
    public let type: ActionType
    /// A text.
    public let text: String
    
    /// Init an attachment action.
    /// - Parameters:
    ///   - name: a name.
    ///   - value: a value.
    ///   - style: a style.
    ///   - type: a type.
    ///   - text: a text.
    public init(
        name: String,
        value: String,
        style: ActionStyle,
        type: ActionType,
        text: String
    ) {
        self.name = name
        self.value = value
        self.style = style
        self.type = type
        self.text = text
    }

    /// Check if the action is cancel button.
    public var isCancel: Bool { value.lowercased() == "cancel" }
    
    /// An attachment action type, e.g. button.
    public enum ActionType: String, Codable {
        case button
    }

    /// An attachment action style, e.g. primary button.
    public enum ActionStyle: String, Codable {
        case `default`
        case primary
    }
}

/// An attachment type.
/// There are some predefined types on backend but any type can be introduced and sent to backend.
public struct AttachmentType: RawRepresentable, Codable, Hashable, ExpressibleByStringLiteral {
    public let rawValue: String

    public init(rawValue: String) {
        self.rawValue = rawValue
    }

    public init(stringLiteral value: String) {
        self.init(rawValue: value)
    }
}

public extension AttachmentType {
    /// Backend specified types.
    static let image = Self(rawValue: "image")
    static let file = Self(rawValue: "file")
    static let giphy = Self(rawValue: "giphy")
    static let video = Self(rawValue: "video")
    static let audio = Self(rawValue: "audio")

    /// Application custom types.
    static let linkPreview = Self(rawValue: "linkPreview")
    /// Is used when attachment with missing `type` comes.
    static let unknown = Self(rawValue: "unknown")
}

/// An attachment file description.
public struct AttachmentFile: Codable, Hashable {
    enum CodingKeys: String, CodingKey, CaseIterable {
        case mimeType = "mime_type"
        case size = "file_size"
    }
    
    /// An attachment file type (see `AttachmentFileType`).
    public let type: AttachmentFileType
    /// A size of the file.
    public let size: Int64
    /// A mime type.
    public let mimeType: String?
    /// A file size formatter.
    public static let sizeFormatter = ByteCountFormatter()
    
    /// A formatted file size.
    public var sizeString: String { AttachmentFile.sizeFormatter.string(fromByteCount: size) }
    
    /// Init an attachment file.
    /// - Parameters:
    ///   - type: a file type.
    ///   - size: a file size.
    ///   - mimeType: a mime type.
    public init(type: AttachmentFileType, size: Int64, mimeType: String?) {
        self.type = type
        self.size = size
        self.mimeType = mimeType
    }

    public init(url: URL) throws {
        guard url.isFileURL else {
            throw ClientError.InvalidAttachmentFileURL(url)
        }

        let fileType = AttachmentFileType(ext: url.pathExtension)
        let attributes = try FileManager.default.attributesOfItem(atPath: url.path)

        self.init(
            type: fileType,
            size: attributes[.size] as? Int64 ?? 0,
            mimeType: fileType.mimeType
        )
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        mimeType = try? container.decodeIfPresent(String.self, forKey: .mimeType)
        
        if let mimeType = mimeType {
            type = AttachmentFileType(mimeType: mimeType)
        } else {
            type = .generic
        }
        
        if let size = try? container.decodeIfPresent(Int64.self, forKey: .size) {
            self.size = size
        } else if let floatSize = try? container.decodeIfPresent(Float64.self, forKey: .size) {
            size = Int64(floatSize.rounded())
        } else {
            size = 0
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(size, forKey: .size)
        try container.encodeIfPresent(mimeType, forKey: .mimeType)
    }
}

/// An attachment file type.
public enum AttachmentFileType: String, Codable, Equatable, CaseIterable {
    /// A file attachment type.
    case generic, csv, doc, pdf, ppt, tar, xls, zip, mp3, mp4, mov, jpeg, png, gif
    
    private static let mimeTypes: [String: AttachmentFileType] = [
        "application/octet-stream": .generic,
        "text/csv": .csv,
        "application/msword": .doc,
        "application/pdf": .pdf,
        "application/vnd.ms-powerpoint": .ppt,
        "application/x-tar": .tar,
        "application/vnd.ms-excel": .xls,
        "application/zip": .zip,
        "audio/mp3": .mp3,
        "video/mp4": .mp4,
        "video/quicktime": .mov,
        "image/jpeg": .jpeg,
        "image/jpg": .jpeg,
        "image/png": .png,
        "image/gif": .gif
    ]
    
    /// Init an attachment file type by mime type.
    ///
    /// - Parameter mimeType: a mime type.
    public init(mimeType: String) {
        self = AttachmentFileType.mimeTypes[mimeType, default: .generic]
    }
    
    /// Init an attachment file type by a file extension.
    ///
    /// - Parameter ext: a file extension.
    public init(ext: String) {
        // We've seen that iOS sometimes uppercases the filename (and also extension)
        // which breaks our file type detection code.
        // We lowercase it for extra safety
        let ext = ext.lowercased()
        if ext == "jpg" {
            self = .jpeg
            return
        }
        
        self = AttachmentFileType(rawValue: ext) ?? .generic
    }
    
    /// Returns a mime type for the file type.
    public var mimeType: String {
        if self == .jpeg {
            return "image/jpeg"
        }
        
        return AttachmentFileType.mimeTypes.first(where: { $1 == self })?.key ?? "application/octet-stream"
    }
}

extension ClientError {
    class InvalidAttachmentFileURL: ClientError {
        init(_ url: URL) {
            super.init("The \(url) is invalid since it is not a file URL.")
        }
    }
}
