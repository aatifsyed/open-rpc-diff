use std::{
    cmp,
    collections::{
        btree_set::{Difference, Intersection},
        BTreeMap, BTreeSet,
    },
    fs::File,
    io,
    path::{Path, PathBuf},
};

use anyhow::Context as _;
use clap::Parser;
use itertools::{EitherOrBoth, Itertools as _};
use json_schema_diff::Change;
use nunny::NonEmpty;
use openrpc_types::{ContentDescriptor, Method, OpenRPC, SpecificationExtensions};
use schemars::schema::{RootSchema, Schema};
use serde::Serialize;
use serde_json::Value;
use summary::{MethodChange, Summary};

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

    let (only_left, common, only_right) = venn(&left_names, &right_names);

    let mut methods = BTreeMap::new();
    let mut compatible = Vec::new();

    for method in common {
        let method = (*method).clone();

        let (left_params, left_return) = &left_methods[&method];
        let (right_params, right_return) = &right_methods[&method];

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
            compatible.push(method);
            continue;
        }
        methods.insert(
            method,
            MethodChange {
                parameter: param_diffs
                    .into_iter()
                    .flatten()
                    .map(|(ix, it)| (ix, it.into()))
                    .collect(),
                result: result_diff.map(Into::into),
            },
        );
    }

    let summary = Summary {
        equivalent: compatible,
        different: methods,
        left: only_left.map(|it| (*it).clone()).collect(),
        right: only_right.map(|it| (*it).clone()).collect(),
    };

    serde_yaml::to_writer(io::stdout(), &summary)?;

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

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
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
    fn json(schema: &Schema, definitions: &BTreeMap<String, Schema>) -> Value {
        serde_json::to_value(&RootSchema {
            meta_schema: None,
            schema: schema.clone().into_object(),
            definitions: definitions.clone(),
        })
        .unwrap()
    }
    let schema_change = nunny::Vec::new(
        json_schema_diff::diff(
            json(&left.schema, left_definitions),
            json(&right.schema, right_definitions),
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

fn venn<'a, T: Ord>(
    left: &'a BTreeSet<T>,
    right: &'a BTreeSet<T>,
) -> (Difference<'a, T>, Intersection<'a, T>, Difference<'a, T>) {
    let only_left = left.difference(right);
    let only_right = right.difference(left);
    let common = left.intersection(right);
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
    serde_path_to_error::deserialize(&mut serde_json::Deserializer::from_reader(file)).context(
        format!("couldn't deserialize file {}", path.as_ref().display()),
    )
}

mod summary {
    use super::RequiredChange;
    use std::collections::BTreeMap;

    use itertools::EitherOrBoth;
    use json_schema_diff::JsonSchemaType;
    use nunny::NonEmpty;
    use serde::Serialize;
    use serde_json::Value;

    #[derive(Serialize)]
    pub struct Summary {
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub equivalent: Vec<String>,
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub different: BTreeMap<String, MethodChange>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub left: Vec<String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub right: Vec<String>,
    }

    #[derive(Serialize)]
    pub struct MethodChange {
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub parameter: BTreeMap<usize, ContentDescriptorChange>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub result: Option<ContentDescriptorChange>,
    }

    #[derive(Serialize)]
    pub struct ContentDescriptorChange {
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub changes: Vec<Change>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub required: Option<RequiredChange>,
    }

    #[derive(Serialize)]
    pub struct Change {
        #[serde(skip_serializing_if = "String::is_empty")]
        pub path: String,
        pub kind: ChangeKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub of: Option<Subject>,
    }

    #[derive(Serialize)]
    #[serde(untagged)]
    pub enum Subject {
        Type(JsonSchemaType),
        Const(Value),
        Property(String),
    }

    #[derive(Serialize)]
    #[serde(rename_all = "kebab-case")]
    pub enum ChangeKind {
        TypeAdd,
        TypeRemove,
        ConstAdd,
        ConstRemove,
        PropertyAdd,
        PropertyRemove,
        RangeAdd,
        RangeRemove,
        RangeChange,
        TupleToArray,
        ArrayToTuple,
        TupleChange,
        RequiredRemove,
        RequiredAdd,
    }

    impl From<json_schema_diff::Change> for Change {
        fn from(value: json_schema_diff::Change) -> Self {
            let json_schema_diff::Change { path, change } = value;

            use json_schema_diff::ChangeKind as Th;
            let (kind, subject) = match change {
                Th::TypeAdd { added } => (ChangeKind::TypeAdd, Some(Subject::Type(added))),
                Th::TypeRemove { removed } => {
                    (ChangeKind::TypeRemove, Some(Subject::Type(removed)))
                }
                Th::ConstAdd { added } => (ChangeKind::ConstAdd, Some(Subject::Const(added))),
                Th::ConstRemove { removed } => {
                    (ChangeKind::ConstRemove, Some(Subject::Const(removed)))
                }
                Th::PropertyAdd {
                    lhs_additional_properties: _,
                    added,
                } => (ChangeKind::PropertyAdd, Some(Subject::Property(added))),
                Th::PropertyRemove {
                    lhs_additional_properties: _,
                    removed,
                } => (ChangeKind::PropertyRemove, Some(Subject::Property(removed))),
                Th::RangeAdd { added: _ } => (ChangeKind::RangeAdd, None),
                Th::RangeRemove { removed: _ } => (ChangeKind::RangeRemove, None),
                Th::RangeChange {
                    old_value: _,
                    new_value: _,
                } => (ChangeKind::RangeChange, None),
                Th::TupleToArray { old_length: _ } => (ChangeKind::TupleToArray, None),
                Th::ArrayToTuple { new_length: _ } => (ChangeKind::ArrayToTuple, None),
                Th::TupleChange { new_length: _ } => (ChangeKind::TupleChange, None),
                Th::RequiredRemove { property } => (
                    ChangeKind::RequiredRemove,
                    Some(Subject::Property(property)),
                ),
                Th::RequiredAdd { property } => {
                    (ChangeKind::RequiredAdd, Some(Subject::Property(property)))
                }
            };
            Self {
                path,
                kind,
                of: subject,
            }
        }
    }

    impl From<EitherOrBoth<NonEmpty<Vec<json_schema_diff::Change>>, RequiredChange>>
        for ContentDescriptorChange
    {
        fn from(
            value: EitherOrBoth<NonEmpty<Vec<json_schema_diff::Change>>, RequiredChange>,
        ) -> Self {
            let (change, required) = value.left_and_right();
            Self {
                changes: change
                    .map(|it| it.into_vec().into_iter().map(Into::into).collect())
                    .unwrap_or_default(),
                required,
            }
        }
    }
}

mod rewrite_schema_references {
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
