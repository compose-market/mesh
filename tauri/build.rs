fn protocol_namespace_from_app_name(app_name: &str) -> String {
    let mut namespace = String::new();
    let mut last_was_separator = false;

    for ch in app_name.chars() {
        if ch.is_ascii_alphanumeric() {
            namespace.push(ch.to_ascii_lowercase());
            last_was_separator = false;
            continue;
        }

        if !namespace.is_empty() && !last_was_separator {
            namespace.push('.');
            last_was_separator = true;
        }
    }

    while namespace.ends_with('.') {
        namespace.pop();
    }

    assert!(
        !namespace.is_empty(),
        "mesh/package.json name must resolve to a non-empty libp2p namespace"
    );
    namespace
}

fn emit_mesh_protocol_namespace() {
    let manifest_dir =
        std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let package_json_path = manifest_dir
        .parent()
        .expect("mesh/tauri has a parent directory")
        .join("package.json");
    println!("cargo:rerun-if-changed={}", package_json_path.display());

    let package_json = std::fs::read_to_string(&package_json_path)
        .expect("failed to read mesh/package.json for protocol namespace");
    let package: serde_json::Value =
        serde_json::from_str(&package_json).expect("mesh/package.json must be valid JSON");
    let app_name = package
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .expect("mesh/package.json must define a non-empty name");
    let namespace = protocol_namespace_from_app_name(app_name);

    println!("cargo:rustc-env=COMPOSE_MESH_PROTOCOL_NAMESPACE={namespace}");
}

fn main() {
    emit_mesh_protocol_namespace();

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
        println!("cargo:rustc-link-lib=framework=CoreLocation");
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
#import <CoreLocation/CoreLocation.h>
#import <CoreGraphics/CoreGraphics.h>
#import <dispatch/dispatch.h>
#include <stdbool.h>

static void compose_mesh_run_on_main(void (^block)(void)) {
  if ([NSThread isMainThread]) {
    block();
  } else {
    dispatch_sync(dispatch_get_main_queue(), block);
  }
}

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

static bool compose_mesh_location_is_authorized(CLAuthorizationStatus status) {
  return status == kCLAuthorizationStatusAuthorizedAlways;
}

static CLAuthorizationStatus compose_mesh_current_location_authorization_status(CLLocationManager *manager) {
  if (![CLLocationManager locationServicesEnabled]) {
    return kCLAuthorizationStatusDenied;
  }

  if (@available(macOS 11.0, *)) {
    return manager.authorizationStatus;
  }

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
  return [CLLocationManager authorizationStatus];
#pragma clang diagnostic pop
}

int compose_mesh_location_authorization_status(void) {
  CLLocationManager *manager = [CLLocationManager new];
  return (int)compose_mesh_current_location_authorization_status(manager);
}

@interface ComposeMeshLocationPermissionDelegate : NSObject<CLLocationManagerDelegate>
@property(nonatomic, strong) dispatch_semaphore_t semaphore;
@end

@implementation ComposeMeshLocationPermissionDelegate
- (void)signalAuthorizationChange {
  if (self.semaphore != nil) {
    dispatch_semaphore_signal(self.semaphore);
  }
}

- (void)locationManagerDidChangeAuthorization:(CLLocationManager *)manager {
  (void)manager;
  [self signalAuthorizationChange];
}

- (void)locationManager:(CLLocationManager *)manager didChangeAuthorizationStatus:(CLAuthorizationStatus)status {
  (void)manager;
  (void)status;
  [self signalAuthorizationChange];
}
@end

bool compose_mesh_request_location_access(void) {
  if (![CLLocationManager locationServicesEnabled]) {
    return false;
  }

  __block BOOL granted = NO;
  compose_mesh_run_on_main(^{
    CLLocationManager *manager = [CLLocationManager new];
    CLAuthorizationStatus current = compose_mesh_current_location_authorization_status(manager);
    if (compose_mesh_location_is_authorized(current)) {
      granted = YES;
      return;
    }

    ComposeMeshLocationPermissionDelegate *delegate = [ComposeMeshLocationPermissionDelegate new];
    delegate.semaphore = dispatch_semaphore_create(0);
    manager.delegate = delegate;
    [manager requestWhenInUseAuthorization];

    dispatch_time_t timeout = dispatch_time(DISPATCH_TIME_NOW, 5 * NSEC_PER_SEC);
    dispatch_semaphore_wait(delegate.semaphore, timeout);
    granted = compose_mesh_location_is_authorized(compose_mesh_current_location_authorization_status(manager));
  });

  return granted;
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
