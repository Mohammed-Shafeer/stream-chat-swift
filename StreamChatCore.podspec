Pod::Spec.new do |spec|
  spec.name = "StreamChatCore"
  spec.version = "1.6.2"
  spec.summary = "Stream iOS Chat Core"
  spec.description = "stream-chat-swift is the official Swift client for Stream Chat, a service for building chat applications."

  spec.homepage = "https://getstream.io/chat/"
  spec.license = { :type => "BSD-3", :file => "LICENSE" }
  spec.author = { "Alexey Bukhtin" => "alexey@getstream.io" }
  spec.social_media_url = "https://getstream.io"
  spec.swift_version = "5.0"
  spec.platform = :ios, "11.0"
  spec.source = { :git => "https://github.com/GetStream/stream-chat-swift.git", :tag => "#{spec.version}" }
  spec.requires_arc = true

  spec.source_files  = "Sources/Core/**/*.swift"

  spec.framework = "Foundation", "UIKit"

  spec.dependency "StreamChatClient"
  spec.dependency "RxSwift", "~> 5.0.0"
  spec.dependency "RxCocoa", "~> 5.0.0"
end
