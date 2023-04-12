#[allow(unused)]
extern crate proc_macro;

use proc_macro2::TokenStream;
use syn::DeriveInput;

struct Field {
    field_ident: proc_macro2::Ident,
    db_name: std::string::String,
    local_name: std::string::String,
    ty: syn::Type,
    required: bool,
}

struct Attribute {
    name: std::string::String,
    value: std::string::String,
}

//region Derive macro 'BigDataTableDerive'

#[proc_macro_derive(BigDataTableDerive, attributes(db_name, required, client, primary_key))]
pub fn big_query_table_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let ast = syn::parse(input).unwrap();
    let tokens = impl_big_query_table_derive(&ast);
    tokens.into()
}

fn impl_big_query_table_derive(ast: &syn::DeriveInput) -> proc_macro2::TokenStream {
    let pk_field = get_pk_field(&ast);
    let client_field = get_client_field(&ast);
    implement_big_query_table_base(&ast, &pk_field, &client_field)
}

fn implement_big_query_table_base(
    ast: &DeriveInput,
    pk_field: &Field,
    client_field: &Field,
) -> proc_macro2::TokenStream {
    let table_ident = &ast.ident;
    let table_name = get_table_name(&ast);
    let impl_get_client = implement_get_client(&client_field);
    let impl_set_client = implement_set_client(&client_field);
    let impl_get_pk_field_name = implement_get_pk_field_name(&pk_field);
    let impl_get_pk_db_name = implement_get_pk_db_name(&pk_field);
    let impl_get_pk_value = implement_get_pk_value(&pk_field);
    let impl_get_query_fields = implement_get_query_fields(&ast);
    let impl_get_table_name = implement_impl_get_table_name(&table_name);
    let impl_set_field_value = implement_set_field_value(&ast);
    let impl_from__query_result_row = implement_from__query_result_row(&ast);
    quote::quote! {
        impl<'a> BigQueryTableBase<'a> for #table_ident<'a> {
            #impl_get_client
            #impl_set_client
            #impl_get_pk_field_name
            #impl_get_pk_db_name
            #impl_get_pk_value
            #impl_get_query_fields
            #impl_get_table_name
            #impl_set_field_value
            #impl_from__query_result_row
        }
    }
}


//region method implementations

fn implement_get_client(client_field: &Field) -> TokenStream {
    let client_ident = client_field.field_ident.clone();
    quote::quote! {
        fn get_client(&self) -> &'a BigqueryClient {
            log::trace!("get_client() self={:?}", self);
            &self.#client_ident
        }
    }
}

fn implement_set_client(client_field: &Field) -> TokenStream {
    let client_ident = client_field.field_ident.clone();
    quote::quote! {
        fn set_client(&mut self, client: &'a BigqueryClient) {
            log::trace!("set_client() self={:?}", self);
            self.#client_ident = client;
        }
    }
}

fn implement_get_pk_field_name(pk_field: &Field) -> TokenStream {
    let pk_local_name = pk_field.local_name.clone();
    quote::quote! {
        fn get_pk_field_name() -> String {
            log::trace!("get_pk_field_name()");
            String::from(#pk_local_name)
        }
    }
}

fn implement_get_pk_db_name(pk_field: &Field) -> TokenStream {
    let pk_db_name = pk_field.db_name.clone();
    quote::quote! {
        fn get_pk_db_name() -> String {
            log::trace!("get_pk_db_name()");
            String::from(#pk_db_name)
        }
    }
}

fn implement_get_pk_value(pk_field: &Field) -> TokenStream {
    let pk_ident = &pk_field.field_ident;
    quote::quote! {
        fn get_pk_value(&self) -> &dyn google_bigquery_v2::data::param_conversion::BigDataValueType {
            log::trace!("get_pk_value() self={:?}", self);
            &self.#pk_ident
        }
    }
}

fn implement_get_query_fields(ast: &DeriveInput) -> TokenStream {
    fn implement_map_insert(f: Field) -> TokenStream {
        let local_name = f.local_name;
        let db_name = f.db_name;
        quote::quote! {
            map.insert(String::from(#local_name),String::from(#db_name));
        }
    }
    let fields = get_fields(&ast.data);
    let pk_field = get_pk_field(&ast);
    let client_ident = get_client_field(&ast).field_ident;
    let fields: Vec<TokenStream> = fields
        .into_iter()
        .filter(|f| {
            f.field_ident != client_ident && f.field_ident != pk_field.field_ident
        })
        .map(implement_map_insert)
        .collect();

    let pk_insert = implement_map_insert(pk_field);

    quote::quote! {
        fn get_query_fields(include_pk: bool) -> std::collections::HashMap<String, String> {
            log::trace!("get_query_fields() include_pk={}", include_pk);
            let mut map = std::collections::HashMap::new();
            if(include_pk) {
                #pk_insert
            }
            #(#fields)*
            map
        }
    }
}

fn implement_impl_get_table_name(table_name: &String) -> TokenStream {
    quote::quote! {
        fn get_table_name() -> String {
            log::trace!("get_table_name()");
            String::from(#table_name)
        }
    }
}

fn implement_set_field_value(ast: &DeriveInput) -> TokenStream {
    fn write_set_field_value(f: Field) -> TokenStream {
        let field_ident = f.field_ident;
        let local_name = f.local_name;
        let field_type = f.ty;
        quote::quote! {
            #local_name => self.#field_ident = #field_type::from_param(value)?,
        }
    }
    let client_ident = get_client_field(&ast).field_ident;
    let fields = get_fields(&ast.data);
    let fields: Vec<TokenStream> = fields
        .into_iter()
        .filter(|f| {
            f.field_ident != client_ident
        })
        .map(write_set_field_value)
        .collect();

    quote::quote! {
        fn set_field_value(&mut self, field_name: &str, value: &serde_json::Value) -> Result<()>{
            log::trace!("set_field_value() self={:?} field_name={} value={:?}", self, field_name, value);
            use google_bigquery_v2::data::param_conversion::ConvertBigQueryParams;
            match field_name {
                #(#fields)*
                _ => return Err(google_bigquery_v2::data::param_conversion::ConversionError::new(format!("Field '{}' not found", field_name)).into())
            }
            Ok(())
        }
    }
}

fn implement_from__query_result_row(ast: &DeriveInput) -> TokenStream {
    fn set_field_value(f: Field) -> TokenStream {
        let field_ident = f.field_ident;
        let local_name = f.local_name;
        let field_type = f.ty;
        let db_name = f.db_name;
        quote::quote! {
            #field_ident: #field_type::from_param(&row[#db_name])?,
        }
    }
    let client_ident = get_client_field(&ast).field_ident;
    let fields = get_fields(&ast.data);
    let fields: Vec<TokenStream> = fields
        .into_iter()
        .filter(|f| {
            f.field_ident != client_ident
        })
        .map(set_field_value)
        .collect();
    quote::quote! {
         fn new_from_query_result_row(
        client: &'a BigqueryClient,
        row: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Self>
        where Self: Sized {
            log::trace!("new_from_query_result_row() client={:?} row={:?}", client, row);
            use google_bigquery_v2::data::param_conversion::ConvertBigQueryParams;
            let result = Self{
                #client_ident: client,
                #(#fields)*
            };
            Ok(result)
         }
     }
}
//endregion

//endregion

//region Helper functions

fn get_table_name(ast: &DeriveInput) -> String {
    for attr in get_struct_attributes(ast) {
        if attr.name.eq("db_name") {
            let tokens = &attr.value;
            return tokens.to_string();
        }
    }
    ast.ident.to_string()
}

fn get_pk_field(ast: &syn::DeriveInput) -> Field {
    let mut pk_fields = get_fields_with_attribute(&ast.data, "primary_key");
    if pk_fields.len() != 1 {
        panic!("Exactly one primary key field must be specified");
    }
    let pk = pk_fields.remove(0);
    pk
}

fn get_client_field(ast: &syn::DeriveInput) -> Field {
    //region client
    let mut client_fields = get_fields_with_attribute(&ast.data, "client");
    if client_fields.len() != 1 {
        panic!("Exactly one client field must be specified");
    }
    let client = client_fields.remove(0);
    //endregion
    client
}

fn get_struct_attributes(ast: &syn::DeriveInput) -> Vec<Attribute> {
    let attrs = &ast.attrs;
    let mut res = vec![];
    for attr in attrs {
        if attr.path().is_ident("db_name") {
            let args: syn::LitStr = attr.parse_args().expect("Failed to parse target name");
            let args = args.value();
            res.push(Attribute {
                name: "db_name".to_string(),
                value: args,
            });
        }
    }
    res
}

fn get_fields(data: &syn::Data) -> Vec<Field> {
    let mut res = vec![];

    match data {
        syn::Data::Struct(ref data_struct) => match data_struct.fields {
            syn::Fields::Named(ref fields_named) => {
                for field in fields_named.named.iter() {
                    if let Some(parsed_field) = parse_local_field(&field, false) {
                        res.push(parsed_field);
                    }
                }
            }
            _ => (),
        },
        _ => panic!("Must be a struct!"),
    };

    return res;
}

fn parse_local_field(field: &syn::Field, include_ignored: bool) -> Option<Field> {
    match &field.ident {
        Some(ident) => {
            let mut name = None;
            let mut required = false;
            let attrs = &field.attrs;
            for attribute in attrs {
                if attribute.path().is_ident("db_ignore") && !include_ignored {
                    return None; //skip this field completely
                }
                if attribute.path().is_ident("db_name") {
                    let args: syn::LitStr =
                        attribute.parse_args().expect("Failed to parse target name");
                    let args = args.value();
                    name = Some(args);
                }
                if attribute.path().is_ident("required") {
                    required = true;
                }
            }

            let local_name = ident.to_string();
            let name = match name {
                None => local_name.clone(),
                Some(n) => n,
            };
            let parsed_field = Field {
                field_ident: ident.clone(),
                local_name,
                db_name: name,
                ty: field.ty.clone(),
                required,
            };
            return Some(parsed_field);
        }
        _ => None,
    }
}

fn get_fields_with_attribute(data: &syn::Data, attribute_name: &str) -> Vec<Field> {
    let mut res = vec![];
    match data {
        // Only process structs
        syn::Data::Struct(ref data_struct) => {
            // Check the kind of fields the struct contains
            match data_struct.fields {
                // Structs with named fields
                syn::Fields::Named(ref fields_named) => {
                    // Iterate over the fields
                    for field in fields_named.named.iter() {
                        if let Some(_) = &field.ident {
                            // Get attributes #[..] on each field
                            for attr in field.attrs.iter() {
                                // Parse the attribute
                                if attr.path().is_ident(attribute_name) {
                                    let parsed_field = parse_local_field(&field, true).unwrap();
                                    res.push(parsed_field);
                                }
                            }
                        }
                    }
                }

                // Struct with unnamed fields
                _ => (),
            }
        }

        // Panic when we don't have a struct
        _ => panic!("Must be a struct"),
    }

    return res;
}

//endregion
