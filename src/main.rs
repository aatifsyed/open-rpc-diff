use std::{
    cmp,
    collections::{
        btree_set::{Difference, Union},
        BTreeMap, BTreeSet,
    },
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::Context as _;
use clap::Parser;
use itertools::{EitherOrBoth, Itertools as _};
use json_schema_diff::Change;
use nunny::NonEmpty;
use openrpc_types::{ContentDescriptor, Method, OpenRPC, SpecificationExtensions};
use schemars::schema::Schema;
use serde::Serialize;
use serde_json::Value;

const NO_DESCRIPTOR: &ContentDescriptor = &ContentDescriptor {
    name: String::new(),
    summary: None,
    description: None,
    required: Some(false),
    schema: Schema::Bool(false),
    deprecated: None,
    extensions: SpecificationExtensions(BTreeMap::new()),
};

#[derive(Parser)]
struct Args {
    left: PathBuf,
    right: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let Args { left, right } = Args::parse();

    let (left_definitions, left_methods) = prepare(left)?;
    let (right_definitions, right_methods) = prepare(right)?;

    let left_names = left_methods.keys().collect();
    let right_names = right_methods.keys().collect();

    let (only_left, common, only_right) = sets(&left_names, &right_names);

    for method in common {
        let (left_params, left_return) = &left_methods[*method];
        let (right_params, right_return) = &right_methods[*method];

        let common_length = cmp::max(left_params.len(), right_params.len());

        let param_diffs = nunny::Vec::new(
            left_params
                .iter()
                .pad_using(common_length, |_ix| NO_DESCRIPTOR)
                .zip(
                    right_params
                        .iter()
                        .pad_using(common_length, |_ix| NO_DESCRIPTOR),
                )
                .enumerate()
                .flat_map(|(ix, (l, r))| {
                    diff(l, r, &left_definitions, &right_definitions).map(|it| (ix, it))
                })
                .collect(),
        )
        .ok();

        let result_diff = diff(
            left_return.as_ref().unwrap_or(NO_DESCRIPTOR),
            right_return.as_ref().unwrap_or(NO_DESCRIPTOR),
            &left_definitions,
            &right_definitions,
        );

        if param_diffs.is_none() && result_diff.is_none() {
            continue;
        }
        println!("method {}", method);
        for (ix, it) in param_diffs.into_iter().flatten() {
            println!("\tparameter {}: {:?}", ix, it)
        }
        if let Some(it) = result_diff {
            println!("\tresult: {:?}", it)
        }
        println!()
    }

    for (methods, ident) in [(only_left, "left"), (only_right, "right")] {
        if let Ok(methods) = nunny::Vec::new(methods.collect()) {
            println!("the following methods are only present on the {}:", ident);
            for it in methods {
                println!("\t{}", it)
            }
            println!()
        }
    }

    Ok(())
}

#[allow(clippy::type_complexity)]
fn prepare(
    path: PathBuf,
) -> anyhow::Result<(
    BTreeMap<String, Schema>,
    BTreeMap<String, (Vec<ContentDescriptor>, Option<ContentDescriptor>)>,
)> {
    let mut document = read(path)?;
    rewrite_schema_references::open_rpc(&mut document);
    let definitions = document
        .components
        .unwrap_or_default()
        .schemas
        .unwrap_or_default();
    let methods = document
        .methods
        .into_iter()
        .map(method)
        .collect::<BTreeMap<_, _>>();
    Ok((definitions, methods))
}

#[derive(Debug)]
enum RequiredChange {
    /// left was [`ContentDescriptor::required`], but right was not
    Left,
    /// right was [`ContentDescriptor::required`], but left was not
    Right,
}

fn diff(
    left: &ContentDescriptor,
    right: &ContentDescriptor,
    left_definitions: &BTreeMap<String, Schema>,
    right_definitions: &BTreeMap<String, Schema>,
) -> Option<EitherOrBoth<NonEmpty<Vec<Change>>, RequiredChange>> {
    #[derive(Serialize)]
    struct Combine<'a> {
        #[serde(flatten)]
        schema: &'a Schema,
        #[serde(flatten)]
        definitions: &'a BTreeMap<String, Schema>,
    }
    impl Combine<'_> {
        fn to_json(&self) -> Value {
            serde_json::to_value(self).unwrap()
        }
    }
    let schema_change = nunny::Vec::new(
        json_schema_diff::diff(
            Combine {
                schema: &left.schema,
                definitions: left_definitions,
            }
            .to_json(),
            Combine {
                schema: &right.schema,
                definitions: right_definitions,
            }
            .to_json(),
        )
        .unwrap(),
    )
    .ok();
    let required_change = match (
        left.required.unwrap_or_default(),
        right.required.unwrap_or_default(),
    ) {
        (true, true) => None,
        (true, false) => Some(RequiredChange::Left),
        (false, true) => Some(RequiredChange::Right),
        (false, false) => None,
    };
    match (required_change, schema_change) {
        (None, None) => None,
        (None, Some(it)) => Some(EitherOrBoth::Left(it)),
        (Some(it), None) => Some(EitherOrBoth::Right(it)),
        (Some(r), Some(s)) => Some(EitherOrBoth::Both(s, r)),
    }
}

fn sets<'a, T: Ord>(
    left: &'a BTreeSet<T>,
    right: &'a BTreeSet<T>,
) -> (Difference<'a, T>, Union<'a, T>, Difference<'a, T>) {
    let only_left = left.difference(right);
    let only_right = right.difference(left);
    let common = left.union(right);
    (only_left, common, only_right)
}

fn method(method: Method) -> (String, (Vec<ContentDescriptor>, Option<ContentDescriptor>)) {
    let Method {
        name,
        tags: _,
        summary: _,
        description: _,
        external_docs: _,
        params,
        result,
        deprecated: _,
        servers: _,
        errors: _,
        param_structure: _,
        examples: _,
        extensions: _,
    } = method;
    (name, (params, result))
}

fn read(path: impl AsRef<Path>) -> anyhow::Result<OpenRPC> {
    let file = File::open(path.as_ref())
        .context(format!("couldn't open file {}", path.as_ref().display()))?;
    serde_path_to_error::deserialize(&mut serde_json::Deserializer::from_reader(file))
        .map_err(Into::into)
}

pub mod rewrite_schema_references {
    use either::Either;
    use openrpc_types::{Components, ContentDescriptor, Method, OpenRPC};
    use schemars::schema::{
        ArrayValidation, ObjectValidation, Schema, SchemaObject, SingleOrVec, SubschemaValidation,
    };
    use std::{collections::BTreeMap, iter};

    pub fn open_rpc(node: &mut OpenRPC) {
        let OpenRPC {
            openrpc: _,
            info: _,
            servers: _,
            methods,
            components,
            external_docs: _,
            extensions: _,
        } = node;
        for Method {
            name: _,
            tags: _,
            summary: _,
            description: _,
            external_docs: _,
            params,
            result,
            deprecated: _,
            servers: _,
            errors: _,
            param_structure: _,
            examples: _,
            extensions: _,
        } in methods
        {
            params.iter_mut().chain(result).for_each(content_descriptor);
        }
        if let Some(Components {
            content_descriptors,
            schemas,
            examples: _,
            errors: _,
            example_pairing_objects: _,
            tags: _,
            extensions: _,
        }) = components
        {
            content_descriptors
                .iter_mut()
                .flat_map(BTreeMap::values_mut)
                .for_each(content_descriptor);
            schemas
                .iter_mut()
                .flat_map(BTreeMap::values_mut)
                .for_each(schema);
        }
    }
    pub fn schema(node: &mut Schema) {
        match node {
            Schema::Bool(_) => {}
            Schema::Object(SchemaObject {
                metadata: _,
                instance_type: _,
                format: _,
                enum_values: _,
                const_value: _,
                subschemas,
                number: _,
                string: _,
                array,
                object,
                reference,
                extensions: _,
            }) => {
                if let Some(reference) = reference {
                    if let Some(path) = reference.strip_prefix("#/components/schemas/") {
                        *reference = format!("#/definitions/{}", path)
                    }
                }

                if let Some(SubschemaValidation {
                    all_of,
                    any_of,
                    one_of,
                    not,
                    if_schema,
                    then_schema,
                    else_schema,
                }) = subschemas.as_deref_mut()
                {
                    iter::empty()
                        .chain(all_of.iter_mut().flatten())
                        .chain(any_of.iter_mut().flatten())
                        .chain(one_of.iter_mut().flatten())
                        .chain(not.as_deref_mut())
                        .chain(if_schema.as_deref_mut())
                        .chain(then_schema.as_deref_mut())
                        .chain(else_schema.as_deref_mut())
                        .for_each(schema)
                }
                if let Some(ArrayValidation {
                    items,
                    additional_items,
                    max_items: _,
                    min_items: _,
                    unique_items: _,
                    contains,
                }) = array.as_deref_mut()
                {
                    items
                        .iter_mut()
                        .flat_map(|it| match it {
                            SingleOrVec::Single(it) => Either::Left(iter::once(&mut **it)),
                            SingleOrVec::Vec(it) => Either::Right(it.iter_mut()),
                        })
                        .chain(additional_items.as_deref_mut())
                        .chain(contains.as_deref_mut())
                        .for_each(schema)
                }
                if let Some(ObjectValidation {
                    max_properties: _,
                    min_properties: _,
                    required: _,
                    properties,
                    pattern_properties,
                    additional_properties,
                    property_names,
                }) = object.as_deref_mut()
                {
                    properties
                        .values_mut()
                        .chain(pattern_properties.values_mut())
                        .chain(additional_properties.as_deref_mut())
                        .chain(property_names.as_deref_mut())
                        .for_each(schema)
                }
            }
        }
    }
    pub fn content_descriptor(node: &mut ContentDescriptor) {
        let ContentDescriptor {
            name: _,
            summary: _,
            description: _,
            required: _,
            schema,
            deprecated: _,
            extensions: _,
        } = node;
        self::schema(schema)
    }
}
