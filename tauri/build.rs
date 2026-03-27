fn main() {
    #[cfg(target_os = "macos")]
    {
        let out_dir =
            std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR is set by Cargo"));
        let bridge_path = out_dir.join("compose_mesh_macos_permissions.m");
        std::fs::write(&bridge_path, MACOS_PERMISSIONS_BRIDGE)
            .expect("failed to write macOS permissions bridge");

        cc::Build::new()
            .file(&bridge_path)
            .flag("-fobjc-arc")
            .compile("compose_mesh_macos_permissions");
        println!("cargo:rustc-link-lib=framework=AppKit");
        println!("cargo:rustc-link-lib=framework=AVFoundation");
        println!("cargo:rustc-link-lib=framework=ApplicationServices");
        println!("cargo:rustc-link-lib=framework=CoreGraphics");
        println!("cargo:rustc-link-lib=framework=Foundation");
    }

    tauri_build::build()
}

#[cfg(target_os = "macos")]
const MACOS_PERMISSIONS_BRIDGE: &str = r#"
#import <AppKit/AppKit.h>
#import <AVFoundation/AVFoundation.h>
#import <ApplicationServices/ApplicationServices.h>
#import <CoreGraphics/CoreGraphics.h>
#import <dispatch/dispatch.h>
#include <stdbool.h>

int compose_mesh_camera_authorization_status(void) {
  return (int)[AVCaptureDevice authorizationStatusForMediaType:AVMediaTypeVideo];
}

bool compose_mesh_request_camera_access(void) {
  __block BOOL granted = NO;
  dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
  [AVCaptureDevice requestAccessForMediaType:AVMediaTypeVideo completionHandler:^(BOOL didGrant) {
    granted = didGrant;
    dispatch_semaphore_signal(semaphore);
  }];
  dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER);
  return granted;
}

int compose_mesh_microphone_authorization_status(void) {
  return (int)[AVCaptureDevice authorizationStatusForMediaType:AVMediaTypeAudio];
}

bool compose_mesh_request_microphone_access(void) {
  __block BOOL granted = NO;
  dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
  [AVCaptureDevice requestAccessForMediaType:AVMediaTypeAudio completionHandler:^(BOOL didGrant) {
    granted = didGrant;
    dispatch_semaphore_signal(semaphore);
  }];
  dispatch_semaphore_wait(semaphore, DISPATCH_TIME_FOREVER);
  return granted;
}

bool compose_mesh_preflight_screen_capture_access(void) {
  return CGPreflightScreenCaptureAccess();
}

bool compose_mesh_request_screen_capture_access(void) {
  return CGRequestScreenCaptureAccess();
}

bool compose_mesh_accessibility_is_trusted(void) {
  return AXIsProcessTrusted();
}

bool compose_mesh_prompt_accessibility_access(void) {
  CFTypeRef values[] = { kCFBooleanTrue };
  CFDictionaryRef options = CFDictionaryCreate(
    kCFAllocatorDefault,
    (const void **)&kAXTrustedCheckOptionPrompt,
    values,
    1,
    &kCFCopyStringDictionaryKeyCallBacks,
    &kCFTypeDictionaryValueCallBacks
  );
  Boolean trusted = AXIsProcessTrustedWithOptions(options);
  CFRelease(options);
  return trusted;
}

bool compose_mesh_open_url(const char *urlCString) {
  if (urlCString == NULL) {
    return false;
  }

  @autoreleasepool {
    NSString *urlString = [NSString stringWithUTF8String:urlCString];
    if (urlString == nil) {
      return false;
    }

    NSURL *url = [NSURL URLWithString:urlString];
    if (url == nil) {
      return false;
    }

    return [[NSWorkspace sharedWorkspace] openURL:url];
  }
}
"#;
