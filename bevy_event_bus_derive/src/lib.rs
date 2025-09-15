extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

/// Derive macro for automatically implementing EventBus functionality.
///
/// This macro:
/// 1. Implements bevy::prelude::Event automatically
/// 2. Registers the type with the EventBusPlugin so you don't need to call register_bus_event
///
/// # Example
///
/// ```
/// #[derive(ExternalBusEvent, Clone, Debug)]
/// struct PlayerLevelUpEvent {
///     entity_id: u64,
///     new_level: u32,
/// }
/// ```
#[proc_macro_derive(ExternalBusEvent)]
pub fn derive_external_bus_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let upper_name = name.to_string().to_uppercase();
    let static_ident = syn::Ident::new(
        &format!("__BEVY_EVENT_BUS_REGISTER_{}", upper_name),
        name.span(),
    );
    let expanded = quote! {
        impl bevy::prelude::Event for #name { type Traversal = (); }
        #[allow(non_upper_case_globals)]
        #[ctor::ctor]
        static #static_ident: () = {
            ::bevy_event_bus::EVENT_REGISTRY
                .lock()
                .unwrap()
                .push(|app| { ::bevy_event_bus::registration::register_event::<#name>(app); });
        };
    };

    TokenStream::from(expanded)
}
