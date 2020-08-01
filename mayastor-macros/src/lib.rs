use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn return_as_is(attr: TokenStream, item: TokenStream) -> TokenStream {
    println!("attr: \"{}\"", attr.to_string());
    println!("item: \"{}\"", item.to_string());
    "pub fn lolz() -> Result<(),()> { Err(()) }".parse().unwrap()
}

fn that() -> Result<(),()> {
    Ok(())
}

#[test]
fn it_works() {
    assert!(that().is_ok());
}