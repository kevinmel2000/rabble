// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(Clone,Default)]
pub struct NodeId {
    // message fields
    name: ::protobuf::SingularField<::std::string::String>,
    addr: ::protobuf::SingularField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for NodeId {}

impl NodeId {
    pub fn new() -> NodeId {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static NodeId {
        static mut instance: ::protobuf::lazy::Lazy<NodeId> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const NodeId,
        };
        unsafe {
            instance.get(|| {
                NodeId {
                    name: ::protobuf::SingularField::none(),
                    addr: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional string name = 1;

    pub fn clear_name(&mut self) {
        self.name.clear();
    }

    pub fn has_name(&self) -> bool {
        self.name.is_some()
    }

    // Param is passed by value, moved
    pub fn set_name(&mut self, v: ::std::string::String) {
        self.name = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_name(&mut self) -> &mut ::std::string::String {
        if self.name.is_none() {
            self.name.set_default();
        };
        self.name.as_mut().unwrap()
    }

    // Take field
    pub fn take_name(&mut self) -> ::std::string::String {
        self.name.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_name(&self) -> &str {
        match self.name.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    // optional string addr = 2;

    pub fn clear_addr(&mut self) {
        self.addr.clear();
    }

    pub fn has_addr(&self) -> bool {
        self.addr.is_some()
    }

    // Param is passed by value, moved
    pub fn set_addr(&mut self, v: ::std::string::String) {
        self.addr = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_addr(&mut self) -> &mut ::std::string::String {
        if self.addr.is_none() {
            self.addr.set_default();
        };
        self.addr.as_mut().unwrap()
    }

    // Take field
    pub fn take_addr(&mut self) -> ::std::string::String {
        self.addr.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_addr(&self) -> &str {
        match self.addr.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }
}

impl ::protobuf::Message for NodeId {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.name));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.addr));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.name {
            my_size += ::protobuf::rt::string_size(1, &value);
        };
        for value in &self.addr {
            my_size += ::protobuf::rt::string_size(2, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.name.as_ref() {
            try!(os.write_string(1, &v));
        };
        if let Some(v) = self.addr.as_ref() {
            try!(os.write_string(2, &v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<NodeId>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for NodeId {
    fn new() -> NodeId {
        NodeId::new()
    }

    fn descriptor_static(_: ::std::option::Option<NodeId>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_string_accessor(
                    "name",
                    NodeId::has_name,
                    NodeId::get_name,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_string_accessor(
                    "addr",
                    NodeId::has_addr,
                    NodeId::get_addr,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<NodeId>(
                    "NodeId",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for NodeId {
    fn clear(&mut self) {
        self.clear_name();
        self.clear_addr();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for NodeId {
    fn eq(&self, other: &NodeId) -> bool {
        self.name == other.name &&
        self.addr == other.addr &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Pids {
    // message fields
    pids: ::protobuf::RepeatedField<Pid>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Pids {}

impl Pids {
    pub fn new() -> Pids {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Pids {
        static mut instance: ::protobuf::lazy::Lazy<Pids> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Pids,
        };
        unsafe {
            instance.get(|| {
                Pids {
                    pids: ::protobuf::RepeatedField::new(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // repeated .Pid pids = 1;

    pub fn clear_pids(&mut self) {
        self.pids.clear();
    }

    // Param is passed by value, moved
    pub fn set_pids(&mut self, v: ::protobuf::RepeatedField<Pid>) {
        self.pids = v;
    }

    // Mutable pointer to the field.
    pub fn mut_pids(&mut self) -> &mut ::protobuf::RepeatedField<Pid> {
        &mut self.pids
    }

    // Take field
    pub fn take_pids(&mut self) -> ::protobuf::RepeatedField<Pid> {
        ::std::mem::replace(&mut self.pids, ::protobuf::RepeatedField::new())
    }

    pub fn get_pids(&self) -> &[Pid] {
        &self.pids
    }
}

impl ::protobuf::Message for Pids {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.pids));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.pids {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.pids {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Pids>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Pids {
    fn new() -> Pids {
        Pids::new()
    }

    fn descriptor_static(_: ::std::option::Option<Pids>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "pids",
                    Pids::get_pids,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Pids>(
                    "Pids",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Pids {
    fn clear(&mut self) {
        self.clear_pids();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Pids {
    fn eq(&self, other: &Pids) -> bool {
        self.pids == other.pids &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Pids {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Pid {
    // message fields
    name: ::protobuf::SingularField<::std::string::String>,
    group: ::protobuf::SingularField<::std::string::String>,
    node: ::protobuf::SingularPtrField<NodeId>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Pid {}

impl Pid {
    pub fn new() -> Pid {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Pid {
        static mut instance: ::protobuf::lazy::Lazy<Pid> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Pid,
        };
        unsafe {
            instance.get(|| {
                Pid {
                    name: ::protobuf::SingularField::none(),
                    group: ::protobuf::SingularField::none(),
                    node: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional string name = 1;

    pub fn clear_name(&mut self) {
        self.name.clear();
    }

    pub fn has_name(&self) -> bool {
        self.name.is_some()
    }

    // Param is passed by value, moved
    pub fn set_name(&mut self, v: ::std::string::String) {
        self.name = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_name(&mut self) -> &mut ::std::string::String {
        if self.name.is_none() {
            self.name.set_default();
        };
        self.name.as_mut().unwrap()
    }

    // Take field
    pub fn take_name(&mut self) -> ::std::string::String {
        self.name.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_name(&self) -> &str {
        match self.name.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    // optional string group = 2;

    pub fn clear_group(&mut self) {
        self.group.clear();
    }

    pub fn has_group(&self) -> bool {
        self.group.is_some()
    }

    // Param is passed by value, moved
    pub fn set_group(&mut self, v: ::std::string::String) {
        self.group = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_group(&mut self) -> &mut ::std::string::String {
        if self.group.is_none() {
            self.group.set_default();
        };
        self.group.as_mut().unwrap()
    }

    // Take field
    pub fn take_group(&mut self) -> ::std::string::String {
        self.group.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_group(&self) -> &str {
        match self.group.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    // optional .NodeId node = 3;

    pub fn clear_node(&mut self) {
        self.node.clear();
    }

    pub fn has_node(&self) -> bool {
        self.node.is_some()
    }

    // Param is passed by value, moved
    pub fn set_node(&mut self, v: NodeId) {
        self.node = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_node(&mut self) -> &mut NodeId {
        if self.node.is_none() {
            self.node.set_default();
        };
        self.node.as_mut().unwrap()
    }

    // Take field
    pub fn take_node(&mut self) -> NodeId {
        self.node.take().unwrap_or_else(|| NodeId::new())
    }

    pub fn get_node(&self) -> &NodeId {
        self.node.as_ref().unwrap_or_else(|| NodeId::default_instance())
    }
}

impl ::protobuf::Message for Pid {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.name));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.group));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.node));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.name {
            my_size += ::protobuf::rt::string_size(1, &value);
        };
        for value in &self.group {
            my_size += ::protobuf::rt::string_size(2, &value);
        };
        for value in &self.node {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.name.as_ref() {
            try!(os.write_string(1, &v));
        };
        if let Some(v) = self.group.as_ref() {
            try!(os.write_string(2, &v));
        };
        if let Some(v) = self.node.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Pid>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Pid {
    fn new() -> Pid {
        Pid::new()
    }

    fn descriptor_static(_: ::std::option::Option<Pid>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_string_accessor(
                    "name",
                    Pid::has_name,
                    Pid::get_name,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_string_accessor(
                    "group",
                    Pid::has_group,
                    Pid::get_group,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "node",
                    Pid::has_node,
                    Pid::get_node,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Pid>(
                    "Pid",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Pid {
    fn clear(&mut self) {
        self.clear_name();
        self.clear_group();
        self.clear_node();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Pid {
    fn eq(&self, other: &Pid) -> bool {
        self.name == other.name &&
        self.group == other.group &&
        self.node == other.node &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Pid {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct CorrelationId {
    // message fields
    pid: ::protobuf::SingularPtrField<Pid>,
    handle: ::std::option::Option<u64>,
    request: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for CorrelationId {}

impl CorrelationId {
    pub fn new() -> CorrelationId {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CorrelationId {
        static mut instance: ::protobuf::lazy::Lazy<CorrelationId> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CorrelationId,
        };
        unsafe {
            instance.get(|| {
                CorrelationId {
                    pid: ::protobuf::SingularPtrField::none(),
                    handle: ::std::option::Option::None,
                    request: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .Pid pid = 1;

    pub fn clear_pid(&mut self) {
        self.pid.clear();
    }

    pub fn has_pid(&self) -> bool {
        self.pid.is_some()
    }

    // Param is passed by value, moved
    pub fn set_pid(&mut self, v: Pid) {
        self.pid = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_pid(&mut self) -> &mut Pid {
        if self.pid.is_none() {
            self.pid.set_default();
        };
        self.pid.as_mut().unwrap()
    }

    // Take field
    pub fn take_pid(&mut self) -> Pid {
        self.pid.take().unwrap_or_else(|| Pid::new())
    }

    pub fn get_pid(&self) -> &Pid {
        self.pid.as_ref().unwrap_or_else(|| Pid::default_instance())
    }

    // optional uint64 handle = 2;

    pub fn clear_handle(&mut self) {
        self.handle = ::std::option::Option::None;
    }

    pub fn has_handle(&self) -> bool {
        self.handle.is_some()
    }

    // Param is passed by value, moved
    pub fn set_handle(&mut self, v: u64) {
        self.handle = ::std::option::Option::Some(v);
    }

    pub fn get_handle(&self) -> u64 {
        self.handle.unwrap_or(0)
    }

    // optional uint64 request = 3;

    pub fn clear_request(&mut self) {
        self.request = ::std::option::Option::None;
    }

    pub fn has_request(&self) -> bool {
        self.request.is_some()
    }

    // Param is passed by value, moved
    pub fn set_request(&mut self, v: u64) {
        self.request = ::std::option::Option::Some(v);
    }

    pub fn get_request(&self) -> u64 {
        self.request.unwrap_or(0)
    }
}

impl ::protobuf::Message for CorrelationId {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.pid));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.handle = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.request = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.pid {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.handle {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in &self.request {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.pid.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.handle {
            try!(os.write_uint64(2, v));
        };
        if let Some(v) = self.request {
            try!(os.write_uint64(3, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<CorrelationId>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for CorrelationId {
    fn new() -> CorrelationId {
        CorrelationId::new()
    }

    fn descriptor_static(_: ::std::option::Option<CorrelationId>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "pid",
                    CorrelationId::has_pid,
                    CorrelationId::get_pid,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "handle",
                    CorrelationId::has_handle,
                    CorrelationId::get_handle,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "request",
                    CorrelationId::has_request,
                    CorrelationId::get_request,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<CorrelationId>(
                    "CorrelationId",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CorrelationId {
    fn clear(&mut self) {
        self.clear_pid();
        self.clear_handle();
        self.clear_request();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for CorrelationId {
    fn eq(&self, other: &CorrelationId) -> bool {
        self.pid == other.pid &&
        self.handle == other.handle &&
        self.request == other.request &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for CorrelationId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Envelope {
    // message fields
    to: ::protobuf::SingularPtrField<Pid>,
    from: ::protobuf::SingularPtrField<Pid>,
    cid: ::protobuf::SingularPtrField<CorrelationId>,
    msg: ::protobuf::SingularPtrField<PbMsg>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Envelope {}

impl Envelope {
    pub fn new() -> Envelope {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Envelope {
        static mut instance: ::protobuf::lazy::Lazy<Envelope> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Envelope,
        };
        unsafe {
            instance.get(|| {
                Envelope {
                    to: ::protobuf::SingularPtrField::none(),
                    from: ::protobuf::SingularPtrField::none(),
                    cid: ::protobuf::SingularPtrField::none(),
                    msg: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .Pid to = 1;

    pub fn clear_to(&mut self) {
        self.to.clear();
    }

    pub fn has_to(&self) -> bool {
        self.to.is_some()
    }

    // Param is passed by value, moved
    pub fn set_to(&mut self, v: Pid) {
        self.to = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_to(&mut self) -> &mut Pid {
        if self.to.is_none() {
            self.to.set_default();
        };
        self.to.as_mut().unwrap()
    }

    // Take field
    pub fn take_to(&mut self) -> Pid {
        self.to.take().unwrap_or_else(|| Pid::new())
    }

    pub fn get_to(&self) -> &Pid {
        self.to.as_ref().unwrap_or_else(|| Pid::default_instance())
    }

    // optional .Pid from = 2;

    pub fn clear_from(&mut self) {
        self.from.clear();
    }

    pub fn has_from(&self) -> bool {
        self.from.is_some()
    }

    // Param is passed by value, moved
    pub fn set_from(&mut self, v: Pid) {
        self.from = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_from(&mut self) -> &mut Pid {
        if self.from.is_none() {
            self.from.set_default();
        };
        self.from.as_mut().unwrap()
    }

    // Take field
    pub fn take_from(&mut self) -> Pid {
        self.from.take().unwrap_or_else(|| Pid::new())
    }

    pub fn get_from(&self) -> &Pid {
        self.from.as_ref().unwrap_or_else(|| Pid::default_instance())
    }

    // optional .CorrelationId cid = 3;

    pub fn clear_cid(&mut self) {
        self.cid.clear();
    }

    pub fn has_cid(&self) -> bool {
        self.cid.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cid(&mut self, v: CorrelationId) {
        self.cid = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cid(&mut self) -> &mut CorrelationId {
        if self.cid.is_none() {
            self.cid.set_default();
        };
        self.cid.as_mut().unwrap()
    }

    // Take field
    pub fn take_cid(&mut self) -> CorrelationId {
        self.cid.take().unwrap_or_else(|| CorrelationId::new())
    }

    pub fn get_cid(&self) -> &CorrelationId {
        self.cid.as_ref().unwrap_or_else(|| CorrelationId::default_instance())
    }

    // optional .PbMsg msg = 4;

    pub fn clear_msg(&mut self) {
        self.msg.clear();
    }

    pub fn has_msg(&self) -> bool {
        self.msg.is_some()
    }

    // Param is passed by value, moved
    pub fn set_msg(&mut self, v: PbMsg) {
        self.msg = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_msg(&mut self) -> &mut PbMsg {
        if self.msg.is_none() {
            self.msg.set_default();
        };
        self.msg.as_mut().unwrap()
    }

    // Take field
    pub fn take_msg(&mut self) -> PbMsg {
        self.msg.take().unwrap_or_else(|| PbMsg::new())
    }

    pub fn get_msg(&self) -> &PbMsg {
        self.msg.as_ref().unwrap_or_else(|| PbMsg::default_instance())
    }
}

impl ::protobuf::Message for Envelope {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.to));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.from));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.cid));
                },
                4 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.msg));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.to {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.from {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.cid {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.msg {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.to.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.from.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.cid.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.msg.as_ref() {
            try!(os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Envelope>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Envelope {
    fn new() -> Envelope {
        Envelope::new()
    }

    fn descriptor_static(_: ::std::option::Option<Envelope>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "to",
                    Envelope::has_to,
                    Envelope::get_to,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "from",
                    Envelope::has_from,
                    Envelope::get_from,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "cid",
                    Envelope::has_cid,
                    Envelope::get_cid,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "msg",
                    Envelope::has_msg,
                    Envelope::get_msg,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Envelope>(
                    "Envelope",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Envelope {
    fn clear(&mut self) {
        self.clear_to();
        self.clear_from();
        self.clear_cid();
        self.clear_msg();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Envelope {
    fn eq(&self, other: &Envelope) -> bool {
        self.to == other.to &&
        self.from == other.from &&
        self.cid == other.cid &&
        self.msg == other.msg &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Envelope {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct PbMsg {
    // message fields
    user_msg: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // message oneof groups
    request: ::std::option::Option<PbMsg_oneof_request>,
    reply: ::std::option::Option<PbMsg_oneof_reply>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PbMsg {}

#[derive(Clone,PartialEq)]
pub enum PbMsg_oneof_request {
    get_metrics(bool),
    start_timer(u64),
    cancel_timer(bool),
    shutdown(bool),
    get_processes(NodeId),
    get_services(NodeId),
}

#[derive(Clone,PartialEq)]
pub enum PbMsg_oneof_reply {
    metrics(Metrics),
    timeout(bool),
    processes(Pids),
    services(Pids),
    members(Members),
}

impl PbMsg {
    pub fn new() -> PbMsg {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PbMsg {
        static mut instance: ::protobuf::lazy::Lazy<PbMsg> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PbMsg,
        };
        unsafe {
            instance.get(|| {
                PbMsg {
                    user_msg: ::protobuf::SingularField::none(),
                    request: ::std::option::Option::None,
                    reply: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bool get_metrics = 1;

    pub fn clear_get_metrics(&mut self) {
        self.request = ::std::option::Option::None;
    }

    pub fn has_get_metrics(&self) -> bool {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::get_metrics(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_get_metrics(&mut self, v: bool) {
        self.request = ::std::option::Option::Some(PbMsg_oneof_request::get_metrics(v))
    }

    pub fn get_get_metrics(&self) -> bool {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::get_metrics(v)) => v,
            _ => false,
        }
    }

    // optional uint64 start_timer = 2;

    pub fn clear_start_timer(&mut self) {
        self.request = ::std::option::Option::None;
    }

    pub fn has_start_timer(&self) -> bool {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::start_timer(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_start_timer(&mut self, v: u64) {
        self.request = ::std::option::Option::Some(PbMsg_oneof_request::start_timer(v))
    }

    pub fn get_start_timer(&self) -> u64 {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::start_timer(v)) => v,
            _ => 0,
        }
    }

    // optional bool cancel_timer = 3;

    pub fn clear_cancel_timer(&mut self) {
        self.request = ::std::option::Option::None;
    }

    pub fn has_cancel_timer(&self) -> bool {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::cancel_timer(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_cancel_timer(&mut self, v: bool) {
        self.request = ::std::option::Option::Some(PbMsg_oneof_request::cancel_timer(v))
    }

    pub fn get_cancel_timer(&self) -> bool {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::cancel_timer(v)) => v,
            _ => false,
        }
    }

    // optional bool shutdown = 4;

    pub fn clear_shutdown(&mut self) {
        self.request = ::std::option::Option::None;
    }

    pub fn has_shutdown(&self) -> bool {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::shutdown(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_shutdown(&mut self, v: bool) {
        self.request = ::std::option::Option::Some(PbMsg_oneof_request::shutdown(v))
    }

    pub fn get_shutdown(&self) -> bool {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::shutdown(v)) => v,
            _ => false,
        }
    }

    // optional .NodeId get_processes = 5;

    pub fn clear_get_processes(&mut self) {
        self.request = ::std::option::Option::None;
    }

    pub fn has_get_processes(&self) -> bool {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::get_processes(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_get_processes(&mut self, v: NodeId) {
        self.request = ::std::option::Option::Some(PbMsg_oneof_request::get_processes(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_get_processes(&mut self) -> &mut NodeId {
        if let ::std::option::Option::Some(PbMsg_oneof_request::get_processes(_)) = self.request {
        } else {
            self.request = ::std::option::Option::Some(PbMsg_oneof_request::get_processes(NodeId::new()));
        }
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::get_processes(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_get_processes(&mut self) -> NodeId {
        if self.has_get_processes() {
            match self.request.take() {
                ::std::option::Option::Some(PbMsg_oneof_request::get_processes(v)) => v,
                _ => panic!(),
            }
        } else {
            NodeId::new()
        }
    }

    pub fn get_get_processes(&self) -> &NodeId {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::get_processes(ref v)) => v,
            _ => NodeId::default_instance(),
        }
    }

    // optional .NodeId get_services = 6;

    pub fn clear_get_services(&mut self) {
        self.request = ::std::option::Option::None;
    }

    pub fn has_get_services(&self) -> bool {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::get_services(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_get_services(&mut self, v: NodeId) {
        self.request = ::std::option::Option::Some(PbMsg_oneof_request::get_services(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_get_services(&mut self) -> &mut NodeId {
        if let ::std::option::Option::Some(PbMsg_oneof_request::get_services(_)) = self.request {
        } else {
            self.request = ::std::option::Option::Some(PbMsg_oneof_request::get_services(NodeId::new()));
        }
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::get_services(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_get_services(&mut self) -> NodeId {
        if self.has_get_services() {
            match self.request.take() {
                ::std::option::Option::Some(PbMsg_oneof_request::get_services(v)) => v,
                _ => panic!(),
            }
        } else {
            NodeId::new()
        }
    }

    pub fn get_get_services(&self) -> &NodeId {
        match self.request {
            ::std::option::Option::Some(PbMsg_oneof_request::get_services(ref v)) => v,
            _ => NodeId::default_instance(),
        }
    }

    // optional .Metrics metrics = 50000;

    pub fn clear_metrics(&mut self) {
        self.reply = ::std::option::Option::None;
    }

    pub fn has_metrics(&self) -> bool {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::metrics(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_metrics(&mut self, v: Metrics) {
        self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::metrics(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_metrics(&mut self) -> &mut Metrics {
        if let ::std::option::Option::Some(PbMsg_oneof_reply::metrics(_)) = self.reply {
        } else {
            self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::metrics(Metrics::new()));
        }
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::metrics(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_metrics(&mut self) -> Metrics {
        if self.has_metrics() {
            match self.reply.take() {
                ::std::option::Option::Some(PbMsg_oneof_reply::metrics(v)) => v,
                _ => panic!(),
            }
        } else {
            Metrics::new()
        }
    }

    pub fn get_metrics(&self) -> &Metrics {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::metrics(ref v)) => v,
            _ => Metrics::default_instance(),
        }
    }

    // optional bool timeout = 50001;

    pub fn clear_timeout(&mut self) {
        self.reply = ::std::option::Option::None;
    }

    pub fn has_timeout(&self) -> bool {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::timeout(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_timeout(&mut self, v: bool) {
        self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::timeout(v))
    }

    pub fn get_timeout(&self) -> bool {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::timeout(v)) => v,
            _ => false,
        }
    }

    // optional .Pids processes = 50002;

    pub fn clear_processes(&mut self) {
        self.reply = ::std::option::Option::None;
    }

    pub fn has_processes(&self) -> bool {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::processes(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_processes(&mut self, v: Pids) {
        self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::processes(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_processes(&mut self) -> &mut Pids {
        if let ::std::option::Option::Some(PbMsg_oneof_reply::processes(_)) = self.reply {
        } else {
            self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::processes(Pids::new()));
        }
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::processes(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_processes(&mut self) -> Pids {
        if self.has_processes() {
            match self.reply.take() {
                ::std::option::Option::Some(PbMsg_oneof_reply::processes(v)) => v,
                _ => panic!(),
            }
        } else {
            Pids::new()
        }
    }

    pub fn get_processes(&self) -> &Pids {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::processes(ref v)) => v,
            _ => Pids::default_instance(),
        }
    }

    // optional .Pids services = 50003;

    pub fn clear_services(&mut self) {
        self.reply = ::std::option::Option::None;
    }

    pub fn has_services(&self) -> bool {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::services(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_services(&mut self, v: Pids) {
        self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::services(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_services(&mut self) -> &mut Pids {
        if let ::std::option::Option::Some(PbMsg_oneof_reply::services(_)) = self.reply {
        } else {
            self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::services(Pids::new()));
        }
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::services(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_services(&mut self) -> Pids {
        if self.has_services() {
            match self.reply.take() {
                ::std::option::Option::Some(PbMsg_oneof_reply::services(v)) => v,
                _ => panic!(),
            }
        } else {
            Pids::new()
        }
    }

    pub fn get_services(&self) -> &Pids {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::services(ref v)) => v,
            _ => Pids::default_instance(),
        }
    }

    // optional .Members members = 50004;

    pub fn clear_members(&mut self) {
        self.reply = ::std::option::Option::None;
    }

    pub fn has_members(&self) -> bool {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::members(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_members(&mut self, v: Members) {
        self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::members(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_members(&mut self) -> &mut Members {
        if let ::std::option::Option::Some(PbMsg_oneof_reply::members(_)) = self.reply {
        } else {
            self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::members(Members::new()));
        }
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::members(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_members(&mut self) -> Members {
        if self.has_members() {
            match self.reply.take() {
                ::std::option::Option::Some(PbMsg_oneof_reply::members(v)) => v,
                _ => panic!(),
            }
        } else {
            Members::new()
        }
    }

    pub fn get_members(&self) -> &Members {
        match self.reply {
            ::std::option::Option::Some(PbMsg_oneof_reply::members(ref v)) => v,
            _ => Members::default_instance(),
        }
    }

    // optional bytes user_msg = 100000;

    pub fn clear_user_msg(&mut self) {
        self.user_msg.clear();
    }

    pub fn has_user_msg(&self) -> bool {
        self.user_msg.is_some()
    }

    // Param is passed by value, moved
    pub fn set_user_msg(&mut self, v: ::std::vec::Vec<u8>) {
        self.user_msg = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_user_msg(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.user_msg.is_none() {
            self.user_msg.set_default();
        };
        self.user_msg.as_mut().unwrap()
    }

    // Take field
    pub fn take_user_msg(&mut self) -> ::std::vec::Vec<u8> {
        self.user_msg.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_user_msg(&self) -> &[u8] {
        match self.user_msg.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }
}

impl ::protobuf::Message for PbMsg {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.request = ::std::option::Option::Some(PbMsg_oneof_request::get_metrics(try!(is.read_bool())));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.request = ::std::option::Option::Some(PbMsg_oneof_request::start_timer(try!(is.read_uint64())));
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.request = ::std::option::Option::Some(PbMsg_oneof_request::cancel_timer(try!(is.read_bool())));
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.request = ::std::option::Option::Some(PbMsg_oneof_request::shutdown(try!(is.read_bool())));
                },
                5 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.request = ::std::option::Option::Some(PbMsg_oneof_request::get_processes(try!(is.read_message())));
                },
                6 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.request = ::std::option::Option::Some(PbMsg_oneof_request::get_services(try!(is.read_message())));
                },
                50000 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::metrics(try!(is.read_message())));
                },
                50001 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::timeout(try!(is.read_bool())));
                },
                50002 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::processes(try!(is.read_message())));
                },
                50003 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::services(try!(is.read_message())));
                },
                50004 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.reply = ::std::option::Option::Some(PbMsg_oneof_reply::members(try!(is.read_message())));
                },
                100000 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.user_msg));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.user_msg {
            my_size += ::protobuf::rt::bytes_size(100000, &value);
        };
        if let ::std::option::Option::Some(ref v) = self.request {
            match v {
                &PbMsg_oneof_request::get_metrics(v) => {
                    my_size += 2;
                },
                &PbMsg_oneof_request::start_timer(v) => {
                    my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
                },
                &PbMsg_oneof_request::cancel_timer(v) => {
                    my_size += 2;
                },
                &PbMsg_oneof_request::shutdown(v) => {
                    my_size += 2;
                },
                &PbMsg_oneof_request::get_processes(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &PbMsg_oneof_request::get_services(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
            };
        };
        if let ::std::option::Option::Some(ref v) = self.reply {
            match v {
                &PbMsg_oneof_reply::metrics(ref v) => {
                    let len = v.compute_size();
                    my_size += 3 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &PbMsg_oneof_reply::timeout(v) => {
                    my_size += 4;
                },
                &PbMsg_oneof_reply::processes(ref v) => {
                    let len = v.compute_size();
                    my_size += 3 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &PbMsg_oneof_reply::services(ref v) => {
                    let len = v.compute_size();
                    my_size += 3 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &PbMsg_oneof_reply::members(ref v) => {
                    let len = v.compute_size();
                    my_size += 3 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
            };
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.user_msg.as_ref() {
            try!(os.write_bytes(100000, &v));
        };
        if let ::std::option::Option::Some(ref v) = self.request {
            match v {
                &PbMsg_oneof_request::get_metrics(v) => {
                    try!(os.write_bool(1, v));
                },
                &PbMsg_oneof_request::start_timer(v) => {
                    try!(os.write_uint64(2, v));
                },
                &PbMsg_oneof_request::cancel_timer(v) => {
                    try!(os.write_bool(3, v));
                },
                &PbMsg_oneof_request::shutdown(v) => {
                    try!(os.write_bool(4, v));
                },
                &PbMsg_oneof_request::get_processes(ref v) => {
                    try!(os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                },
                &PbMsg_oneof_request::get_services(ref v) => {
                    try!(os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                },
            };
        };
        if let ::std::option::Option::Some(ref v) = self.reply {
            match v {
                &PbMsg_oneof_reply::metrics(ref v) => {
                    try!(os.write_tag(50000, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                },
                &PbMsg_oneof_reply::timeout(v) => {
                    try!(os.write_bool(50001, v));
                },
                &PbMsg_oneof_reply::processes(ref v) => {
                    try!(os.write_tag(50002, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                },
                &PbMsg_oneof_reply::services(ref v) => {
                    try!(os.write_tag(50003, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                },
                &PbMsg_oneof_reply::members(ref v) => {
                    try!(os.write_tag(50004, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                },
            };
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<PbMsg>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for PbMsg {
    fn new() -> PbMsg {
        PbMsg::new()
    }

    fn descriptor_static(_: ::std::option::Option<PbMsg>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "get_metrics",
                    PbMsg::has_get_metrics,
                    PbMsg::get_get_metrics,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "start_timer",
                    PbMsg::has_start_timer,
                    PbMsg::get_start_timer,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "cancel_timer",
                    PbMsg::has_cancel_timer,
                    PbMsg::get_cancel_timer,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "shutdown",
                    PbMsg::has_shutdown,
                    PbMsg::get_shutdown,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "get_processes",
                    PbMsg::has_get_processes,
                    PbMsg::get_get_processes,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "get_services",
                    PbMsg::has_get_services,
                    PbMsg::get_get_services,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "metrics",
                    PbMsg::has_metrics,
                    PbMsg::get_metrics,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "timeout",
                    PbMsg::has_timeout,
                    PbMsg::get_timeout,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "processes",
                    PbMsg::has_processes,
                    PbMsg::get_processes,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "services",
                    PbMsg::has_services,
                    PbMsg::get_services,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "members",
                    PbMsg::has_members,
                    PbMsg::get_members,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "user_msg",
                    PbMsg::has_user_msg,
                    PbMsg::get_user_msg,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PbMsg>(
                    "PbMsg",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PbMsg {
    fn clear(&mut self) {
        self.clear_get_metrics();
        self.clear_start_timer();
        self.clear_cancel_timer();
        self.clear_shutdown();
        self.clear_get_processes();
        self.clear_get_services();
        self.clear_metrics();
        self.clear_timeout();
        self.clear_processes();
        self.clear_services();
        self.clear_members();
        self.clear_user_msg();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for PbMsg {
    fn eq(&self, other: &PbMsg) -> bool {
        self.user_msg == other.user_msg &&
        self.request == other.request &&
        self.reply == other.reply &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for PbMsg {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Members {
    // message fields
    members: ::protobuf::RepeatedField<Member>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Members {}

impl Members {
    pub fn new() -> Members {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Members {
        static mut instance: ::protobuf::lazy::Lazy<Members> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Members,
        };
        unsafe {
            instance.get(|| {
                Members {
                    members: ::protobuf::RepeatedField::new(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // repeated .Member members = 1;

    pub fn clear_members(&mut self) {
        self.members.clear();
    }

    // Param is passed by value, moved
    pub fn set_members(&mut self, v: ::protobuf::RepeatedField<Member>) {
        self.members = v;
    }

    // Mutable pointer to the field.
    pub fn mut_members(&mut self) -> &mut ::protobuf::RepeatedField<Member> {
        &mut self.members
    }

    // Take field
    pub fn take_members(&mut self) -> ::protobuf::RepeatedField<Member> {
        ::std::mem::replace(&mut self.members, ::protobuf::RepeatedField::new())
    }

    pub fn get_members(&self) -> &[Member] {
        &self.members
    }
}

impl ::protobuf::Message for Members {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.members));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.members {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.members {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Members>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Members {
    fn new() -> Members {
        Members::new()
    }

    fn descriptor_static(_: ::std::option::Option<Members>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "members",
                    Members::get_members,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Members>(
                    "Members",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Members {
    fn clear(&mut self) {
        self.clear_members();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Members {
    fn eq(&self, other: &Members) -> bool {
        self.members == other.members &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Members {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Member {
    // message fields
    node: ::protobuf::SingularPtrField<NodeId>,
    connected: ::std::option::Option<bool>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Member {}

impl Member {
    pub fn new() -> Member {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Member {
        static mut instance: ::protobuf::lazy::Lazy<Member> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Member,
        };
        unsafe {
            instance.get(|| {
                Member {
                    node: ::protobuf::SingularPtrField::none(),
                    connected: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .NodeId node = 1;

    pub fn clear_node(&mut self) {
        self.node.clear();
    }

    pub fn has_node(&self) -> bool {
        self.node.is_some()
    }

    // Param is passed by value, moved
    pub fn set_node(&mut self, v: NodeId) {
        self.node = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_node(&mut self) -> &mut NodeId {
        if self.node.is_none() {
            self.node.set_default();
        };
        self.node.as_mut().unwrap()
    }

    // Take field
    pub fn take_node(&mut self) -> NodeId {
        self.node.take().unwrap_or_else(|| NodeId::new())
    }

    pub fn get_node(&self) -> &NodeId {
        self.node.as_ref().unwrap_or_else(|| NodeId::default_instance())
    }

    // optional bool connected = 2;

    pub fn clear_connected(&mut self) {
        self.connected = ::std::option::Option::None;
    }

    pub fn has_connected(&self) -> bool {
        self.connected.is_some()
    }

    // Param is passed by value, moved
    pub fn set_connected(&mut self, v: bool) {
        self.connected = ::std::option::Option::Some(v);
    }

    pub fn get_connected(&self) -> bool {
        self.connected.unwrap_or(false)
    }
}

impl ::protobuf::Message for Member {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.node));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_bool());
                    self.connected = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.node {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if self.connected.is_some() {
            my_size += 2;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.node.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.connected {
            try!(os.write_bool(2, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Member>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Member {
    fn new() -> Member {
        Member::new()
    }

    fn descriptor_static(_: ::std::option::Option<Member>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "node",
                    Member::has_node,
                    Member::get_node,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "connected",
                    Member::has_connected,
                    Member::get_connected,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Member>(
                    "Member",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Member {
    fn clear(&mut self) {
        self.clear_node();
        self.clear_connected();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Member {
    fn eq(&self, other: &Member) -> bool {
        self.node == other.node &&
        self.connected == other.connected &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Member {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Metrics {
    // message fields
    metrics: ::protobuf::RepeatedField<Metric>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Metrics {}

impl Metrics {
    pub fn new() -> Metrics {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Metrics {
        static mut instance: ::protobuf::lazy::Lazy<Metrics> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Metrics,
        };
        unsafe {
            instance.get(|| {
                Metrics {
                    metrics: ::protobuf::RepeatedField::new(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // repeated .Metric metrics = 1;

    pub fn clear_metrics(&mut self) {
        self.metrics.clear();
    }

    // Param is passed by value, moved
    pub fn set_metrics(&mut self, v: ::protobuf::RepeatedField<Metric>) {
        self.metrics = v;
    }

    // Mutable pointer to the field.
    pub fn mut_metrics(&mut self) -> &mut ::protobuf::RepeatedField<Metric> {
        &mut self.metrics
    }

    // Take field
    pub fn take_metrics(&mut self) -> ::protobuf::RepeatedField<Metric> {
        ::std::mem::replace(&mut self.metrics, ::protobuf::RepeatedField::new())
    }

    pub fn get_metrics(&self) -> &[Metric] {
        &self.metrics
    }
}

impl ::protobuf::Message for Metrics {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.metrics));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.metrics {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in &self.metrics {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Metrics>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Metrics {
    fn new() -> Metrics {
        Metrics::new()
    }

    fn descriptor_static(_: ::std::option::Option<Metrics>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "metrics",
                    Metrics::get_metrics,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Metrics>(
                    "Metrics",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Metrics {
    fn clear(&mut self) {
        self.clear_metrics();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Metrics {
    fn eq(&self, other: &Metrics) -> bool {
        self.metrics == other.metrics &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Metric {
    // message oneof groups
    metric: ::std::option::Option<Metric_oneof_metric>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Metric {}

#[derive(Clone,PartialEq)]
pub enum Metric_oneof_metric {
    gauge(i64),
    counter(u64),
    v2_serialized_histogram(::std::vec::Vec<u8>),
}

impl Metric {
    pub fn new() -> Metric {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Metric {
        static mut instance: ::protobuf::lazy::Lazy<Metric> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Metric,
        };
        unsafe {
            instance.get(|| {
                Metric {
                    metric: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional int64 gauge = 1;

    pub fn clear_gauge(&mut self) {
        self.metric = ::std::option::Option::None;
    }

    pub fn has_gauge(&self) -> bool {
        match self.metric {
            ::std::option::Option::Some(Metric_oneof_metric::gauge(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_gauge(&mut self, v: i64) {
        self.metric = ::std::option::Option::Some(Metric_oneof_metric::gauge(v))
    }

    pub fn get_gauge(&self) -> i64 {
        match self.metric {
            ::std::option::Option::Some(Metric_oneof_metric::gauge(v)) => v,
            _ => 0,
        }
    }

    // optional uint64 counter = 2;

    pub fn clear_counter(&mut self) {
        self.metric = ::std::option::Option::None;
    }

    pub fn has_counter(&self) -> bool {
        match self.metric {
            ::std::option::Option::Some(Metric_oneof_metric::counter(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_counter(&mut self, v: u64) {
        self.metric = ::std::option::Option::Some(Metric_oneof_metric::counter(v))
    }

    pub fn get_counter(&self) -> u64 {
        match self.metric {
            ::std::option::Option::Some(Metric_oneof_metric::counter(v)) => v,
            _ => 0,
        }
    }

    // optional bytes v2_serialized_histogram = 3;

    pub fn clear_v2_serialized_histogram(&mut self) {
        self.metric = ::std::option::Option::None;
    }

    pub fn has_v2_serialized_histogram(&self) -> bool {
        match self.metric {
            ::std::option::Option::Some(Metric_oneof_metric::v2_serialized_histogram(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_v2_serialized_histogram(&mut self, v: ::std::vec::Vec<u8>) {
        self.metric = ::std::option::Option::Some(Metric_oneof_metric::v2_serialized_histogram(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_v2_serialized_histogram(&mut self) -> &mut ::std::vec::Vec<u8> {
        if let ::std::option::Option::Some(Metric_oneof_metric::v2_serialized_histogram(_)) = self.metric {
        } else {
            self.metric = ::std::option::Option::Some(Metric_oneof_metric::v2_serialized_histogram(::std::vec::Vec::new()));
        }
        match self.metric {
            ::std::option::Option::Some(Metric_oneof_metric::v2_serialized_histogram(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_v2_serialized_histogram(&mut self) -> ::std::vec::Vec<u8> {
        if self.has_v2_serialized_histogram() {
            match self.metric.take() {
                ::std::option::Option::Some(Metric_oneof_metric::v2_serialized_histogram(v)) => v,
                _ => panic!(),
            }
        } else {
            ::std::vec::Vec::new()
        }
    }

    pub fn get_v2_serialized_histogram(&self) -> &[u8] {
        match self.metric {
            ::std::option::Option::Some(Metric_oneof_metric::v2_serialized_histogram(ref v)) => v,
            _ => &[],
        }
    }
}

impl ::protobuf::Message for Metric {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.metric = ::std::option::Option::Some(Metric_oneof_metric::gauge(try!(is.read_int64())));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.metric = ::std::option::Option::Some(Metric_oneof_metric::counter(try!(is.read_uint64())));
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.metric = ::std::option::Option::Some(Metric_oneof_metric::v2_serialized_histogram(try!(is.read_bytes())));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let ::std::option::Option::Some(ref v) = self.metric {
            match v {
                &Metric_oneof_metric::gauge(v) => {
                    my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
                },
                &Metric_oneof_metric::counter(v) => {
                    my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
                },
                &Metric_oneof_metric::v2_serialized_histogram(ref v) => {
                    my_size += ::protobuf::rt::bytes_size(3, &v);
                },
            };
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let ::std::option::Option::Some(ref v) = self.metric {
            match v {
                &Metric_oneof_metric::gauge(v) => {
                    try!(os.write_int64(1, v));
                },
                &Metric_oneof_metric::counter(v) => {
                    try!(os.write_uint64(2, v));
                },
                &Metric_oneof_metric::v2_serialized_histogram(ref v) => {
                    try!(os.write_bytes(3, v));
                },
            };
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Metric>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Metric {
    fn new() -> Metric {
        Metric::new()
    }

    fn descriptor_static(_: ::std::option::Option<Metric>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_i64_accessor(
                    "gauge",
                    Metric::has_gauge,
                    Metric::get_gauge,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "counter",
                    Metric::has_counter,
                    Metric::get_counter,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "v2_serialized_histogram",
                    Metric::has_v2_serialized_histogram,
                    Metric::get_v2_serialized_histogram,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Metric>(
                    "Metric",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Metric {
    fn clear(&mut self) {
        self.clear_gauge();
        self.clear_counter();
        self.clear_v2_serialized_histogram();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Metric {
    fn eq(&self, other: &Metric) -> bool {
        self.metric == other.metric &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Metric {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct ClusterServerMsg {
    // message oneof groups
    msg: ::std::option::Option<ClusterServerMsg_oneof_msg>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ClusterServerMsg {}

#[derive(Clone,PartialEq)]
pub enum ClusterServerMsg_oneof_msg {
    orset(MemberORSet),
    Ping(bool),
    envelope(Envelope),
    Delta(::std::vec::Vec<u8>),
}

impl ClusterServerMsg {
    pub fn new() -> ClusterServerMsg {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ClusterServerMsg {
        static mut instance: ::protobuf::lazy::Lazy<ClusterServerMsg> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ClusterServerMsg,
        };
        unsafe {
            instance.get(|| {
                ClusterServerMsg {
                    msg: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .MemberORSet orset = 1;

    pub fn clear_orset(&mut self) {
        self.msg = ::std::option::Option::None;
    }

    pub fn has_orset(&self) -> bool {
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::orset(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_orset(&mut self, v: MemberORSet) {
        self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::orset(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_orset(&mut self) -> &mut MemberORSet {
        if let ::std::option::Option::Some(ClusterServerMsg_oneof_msg::orset(_)) = self.msg {
        } else {
            self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::orset(MemberORSet::new()));
        }
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::orset(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_orset(&mut self) -> MemberORSet {
        if self.has_orset() {
            match self.msg.take() {
                ::std::option::Option::Some(ClusterServerMsg_oneof_msg::orset(v)) => v,
                _ => panic!(),
            }
        } else {
            MemberORSet::new()
        }
    }

    pub fn get_orset(&self) -> &MemberORSet {
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::orset(ref v)) => v,
            _ => MemberORSet::default_instance(),
        }
    }

    // optional bool Ping = 2;

    pub fn clear_Ping(&mut self) {
        self.msg = ::std::option::Option::None;
    }

    pub fn has_Ping(&self) -> bool {
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Ping(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_Ping(&mut self, v: bool) {
        self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Ping(v))
    }

    pub fn get_Ping(&self) -> bool {
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Ping(v)) => v,
            _ => false,
        }
    }

    // optional .Envelope envelope = 3;

    pub fn clear_envelope(&mut self) {
        self.msg = ::std::option::Option::None;
    }

    pub fn has_envelope(&self) -> bool {
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::envelope(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_envelope(&mut self, v: Envelope) {
        self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::envelope(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_envelope(&mut self) -> &mut Envelope {
        if let ::std::option::Option::Some(ClusterServerMsg_oneof_msg::envelope(_)) = self.msg {
        } else {
            self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::envelope(Envelope::new()));
        }
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::envelope(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_envelope(&mut self) -> Envelope {
        if self.has_envelope() {
            match self.msg.take() {
                ::std::option::Option::Some(ClusterServerMsg_oneof_msg::envelope(v)) => v,
                _ => panic!(),
            }
        } else {
            Envelope::new()
        }
    }

    pub fn get_envelope(&self) -> &Envelope {
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::envelope(ref v)) => v,
            _ => Envelope::default_instance(),
        }
    }

    // optional bytes Delta = 4;

    pub fn clear_Delta(&mut self) {
        self.msg = ::std::option::Option::None;
    }

    pub fn has_Delta(&self) -> bool {
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Delta(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_Delta(&mut self, v: ::std::vec::Vec<u8>) {
        self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Delta(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_Delta(&mut self) -> &mut ::std::vec::Vec<u8> {
        if let ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Delta(_)) = self.msg {
        } else {
            self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Delta(::std::vec::Vec::new()));
        }
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Delta(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_Delta(&mut self) -> ::std::vec::Vec<u8> {
        if self.has_Delta() {
            match self.msg.take() {
                ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Delta(v)) => v,
                _ => panic!(),
            }
        } else {
            ::std::vec::Vec::new()
        }
    }

    pub fn get_Delta(&self) -> &[u8] {
        match self.msg {
            ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Delta(ref v)) => v,
            _ => &[],
        }
    }
}

impl ::protobuf::Message for ClusterServerMsg {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::orset(try!(is.read_message())));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Ping(try!(is.read_bool())));
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::envelope(try!(is.read_message())));
                },
                4 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    self.msg = ::std::option::Option::Some(ClusterServerMsg_oneof_msg::Delta(try!(is.read_bytes())));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let ::std::option::Option::Some(ref v) = self.msg {
            match v {
                &ClusterServerMsg_oneof_msg::orset(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &ClusterServerMsg_oneof_msg::Ping(v) => {
                    my_size += 2;
                },
                &ClusterServerMsg_oneof_msg::envelope(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                },
                &ClusterServerMsg_oneof_msg::Delta(ref v) => {
                    my_size += ::protobuf::rt::bytes_size(4, &v);
                },
            };
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let ::std::option::Option::Some(ref v) = self.msg {
            match v {
                &ClusterServerMsg_oneof_msg::orset(ref v) => {
                    try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                },
                &ClusterServerMsg_oneof_msg::Ping(v) => {
                    try!(os.write_bool(2, v));
                },
                &ClusterServerMsg_oneof_msg::envelope(ref v) => {
                    try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                },
                &ClusterServerMsg_oneof_msg::Delta(ref v) => {
                    try!(os.write_bytes(4, v));
                },
            };
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<ClusterServerMsg>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ClusterServerMsg {
    fn new() -> ClusterServerMsg {
        ClusterServerMsg::new()
    }

    fn descriptor_static(_: ::std::option::Option<ClusterServerMsg>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "orset",
                    ClusterServerMsg::has_orset,
                    ClusterServerMsg::get_orset,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "Ping",
                    ClusterServerMsg::has_Ping,
                    ClusterServerMsg::get_Ping,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "envelope",
                    ClusterServerMsg::has_envelope,
                    ClusterServerMsg::get_envelope,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "Delta",
                    ClusterServerMsg::has_Delta,
                    ClusterServerMsg::get_Delta,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ClusterServerMsg>(
                    "ClusterServerMsg",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ClusterServerMsg {
    fn clear(&mut self) {
        self.clear_orset();
        self.clear_Ping();
        self.clear_envelope();
        self.clear_Delta();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for ClusterServerMsg {
    fn eq(&self, other: &ClusterServerMsg) -> bool {
        self.msg == other.msg &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for ClusterServerMsg {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct MemberORSet {
    // message fields
    from: ::protobuf::SingularPtrField<NodeId>,
    orset: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for MemberORSet {}

impl MemberORSet {
    pub fn new() -> MemberORSet {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static MemberORSet {
        static mut instance: ::protobuf::lazy::Lazy<MemberORSet> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const MemberORSet,
        };
        unsafe {
            instance.get(|| {
                MemberORSet {
                    from: ::protobuf::SingularPtrField::none(),
                    orset: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .NodeId from = 1;

    pub fn clear_from(&mut self) {
        self.from.clear();
    }

    pub fn has_from(&self) -> bool {
        self.from.is_some()
    }

    // Param is passed by value, moved
    pub fn set_from(&mut self, v: NodeId) {
        self.from = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_from(&mut self) -> &mut NodeId {
        if self.from.is_none() {
            self.from.set_default();
        };
        self.from.as_mut().unwrap()
    }

    // Take field
    pub fn take_from(&mut self) -> NodeId {
        self.from.take().unwrap_or_else(|| NodeId::new())
    }

    pub fn get_from(&self) -> &NodeId {
        self.from.as_ref().unwrap_or_else(|| NodeId::default_instance())
    }

    // optional bytes orset = 2;

    pub fn clear_orset(&mut self) {
        self.orset.clear();
    }

    pub fn has_orset(&self) -> bool {
        self.orset.is_some()
    }

    // Param is passed by value, moved
    pub fn set_orset(&mut self, v: ::std::vec::Vec<u8>) {
        self.orset = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_orset(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.orset.is_none() {
            self.orset.set_default();
        };
        self.orset.as_mut().unwrap()
    }

    // Take field
    pub fn take_orset(&mut self) -> ::std::vec::Vec<u8> {
        self.orset.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_orset(&self) -> &[u8] {
        match self.orset.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }
}

impl ::protobuf::Message for MemberORSet {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.from));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.orset));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in &self.from {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in &self.orset {
            my_size += ::protobuf::rt::bytes_size(2, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.from.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.orset.as_ref() {
            try!(os.write_bytes(2, &v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<MemberORSet>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for MemberORSet {
    fn new() -> MemberORSet {
        MemberORSet::new()
    }

    fn descriptor_static(_: ::std::option::Option<MemberORSet>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "from",
                    MemberORSet::has_from,
                    MemberORSet::get_from,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "orset",
                    MemberORSet::has_orset,
                    MemberORSet::get_orset,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<MemberORSet>(
                    "MemberORSet",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for MemberORSet {
    fn clear(&mut self) {
        self.clear_from();
        self.clear_orset();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for MemberORSet {
    fn eq(&self, other: &MemberORSet) -> bool {
        self.from == other.from &&
        self.orset == other.orset &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for MemberORSet {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

static file_descriptor_proto_data: &'static [u8] = &[
    0x0a, 0x11, 0x70, 0x62, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x73, 0x2e, 0x70, 0x72,
    0x6f, 0x74, 0x6f, 0x22, 0x30, 0x0a, 0x06, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x12, 0x12, 0x0a,
    0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d,
    0x65, 0x12, 0x12, 0x0a, 0x04, 0x61, 0x64, 0x64, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
    0x04, 0x61, 0x64, 0x64, 0x72, 0x22, 0x20, 0x0a, 0x04, 0x50, 0x69, 0x64, 0x73, 0x12, 0x18, 0x0a,
    0x04, 0x70, 0x69, 0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x50, 0x69,
    0x64, 0x52, 0x04, 0x70, 0x69, 0x64, 0x73, 0x22, 0x4c, 0x0a, 0x03, 0x50, 0x69, 0x64, 0x12, 0x12,
    0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61,
    0x6d, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x18, 0x02, 0x20, 0x01, 0x28,
    0x09, 0x52, 0x05, 0x67, 0x72, 0x6f, 0x75, 0x70, 0x12, 0x1b, 0x0a, 0x04, 0x6e, 0x6f, 0x64, 0x65,
    0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x52,
    0x04, 0x6e, 0x6f, 0x64, 0x65, 0x22, 0x59, 0x0a, 0x0d, 0x43, 0x6f, 0x72, 0x72, 0x65, 0x6c, 0x61,
    0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x16, 0x0a, 0x03, 0x70, 0x69, 0x64, 0x18, 0x01, 0x20,
    0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x50, 0x69, 0x64, 0x52, 0x03, 0x70, 0x69, 0x64, 0x12, 0x16,
    0x0a, 0x06, 0x68, 0x61, 0x6e, 0x64, 0x6c, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x06,
    0x68, 0x61, 0x6e, 0x64, 0x6c, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73,
    0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
    0x22, 0x76, 0x0a, 0x08, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x12, 0x14, 0x0a, 0x02,
    0x74, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x04, 0x2e, 0x50, 0x69, 0x64, 0x52, 0x02,
    0x74, 0x6f, 0x12, 0x18, 0x0a, 0x04, 0x66, 0x72, 0x6f, 0x6d, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
    0x32, 0x04, 0x2e, 0x50, 0x69, 0x64, 0x52, 0x04, 0x66, 0x72, 0x6f, 0x6d, 0x12, 0x20, 0x0a, 0x03,
    0x63, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x43, 0x6f, 0x72, 0x72,
    0x65, 0x6c, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x52, 0x03, 0x63, 0x69, 0x64, 0x12, 0x18,
    0x0a, 0x03, 0x6d, 0x73, 0x67, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x06, 0x2e, 0x50, 0x62,
    0x4d, 0x73, 0x67, 0x52, 0x03, 0x6d, 0x73, 0x67, 0x22, 0xdd, 0x03, 0x0a, 0x05, 0x50, 0x62, 0x4d,
    0x73, 0x67, 0x12, 0x21, 0x0a, 0x0b, 0x67, 0x65, 0x74, 0x5f, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63,
    0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x48, 0x00, 0x52, 0x0a, 0x67, 0x65, 0x74, 0x4d, 0x65,
    0x74, 0x72, 0x69, 0x63, 0x73, 0x12, 0x21, 0x0a, 0x0b, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74,
    0x69, 0x6d, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x48, 0x00, 0x52, 0x0a, 0x73, 0x74,
    0x61, 0x72, 0x74, 0x54, 0x69, 0x6d, 0x65, 0x72, 0x12, 0x23, 0x0a, 0x0c, 0x63, 0x61, 0x6e, 0x63,
    0x65, 0x6c, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x48, 0x00,
    0x52, 0x0b, 0x63, 0x61, 0x6e, 0x63, 0x65, 0x6c, 0x54, 0x69, 0x6d, 0x65, 0x72, 0x12, 0x1c, 0x0a,
    0x08, 0x73, 0x68, 0x75, 0x74, 0x64, 0x6f, 0x77, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x48,
    0x00, 0x52, 0x08, 0x73, 0x68, 0x75, 0x74, 0x64, 0x6f, 0x77, 0x6e, 0x12, 0x2e, 0x0a, 0x0d, 0x67,
    0x65, 0x74, 0x5f, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x73, 0x18, 0x05, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x07, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x48, 0x00, 0x52, 0x0c, 0x67,
    0x65, 0x74, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x73, 0x12, 0x2c, 0x0a, 0x0c, 0x67,
    0x65, 0x74, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x18, 0x06, 0x20, 0x01, 0x28,
    0x0b, 0x32, 0x07, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x48, 0x00, 0x52, 0x0b, 0x67, 0x65,
    0x74, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x12, 0x26, 0x0a, 0x07, 0x6d, 0x65, 0x74,
    0x72, 0x69, 0x63, 0x73, 0x18, 0xd0, 0x86, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x08, 0x2e, 0x4d,
    0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x48, 0x01, 0x52, 0x07, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63,
    0x73, 0x12, 0x1c, 0x0a, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0xd1, 0x86, 0x03,
    0x20, 0x01, 0x28, 0x08, 0x48, 0x01, 0x52, 0x07, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x12,
    0x27, 0x0a, 0x09, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x73, 0x18, 0xd2, 0x86, 0x03,
    0x20, 0x01, 0x28, 0x0b, 0x32, 0x05, 0x2e, 0x50, 0x69, 0x64, 0x73, 0x48, 0x01, 0x52, 0x09, 0x70,
    0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x73, 0x12, 0x25, 0x0a, 0x08, 0x73, 0x65, 0x72, 0x76,
    0x69, 0x63, 0x65, 0x73, 0x18, 0xd3, 0x86, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x05, 0x2e, 0x50,
    0x69, 0x64, 0x73, 0x48, 0x01, 0x52, 0x08, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x73, 0x12,
    0x26, 0x0a, 0x07, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x18, 0xd4, 0x86, 0x03, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x08, 0x2e, 0x4d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x48, 0x01, 0x52, 0x07,
    0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x12, 0x1b, 0x0a, 0x08, 0x75, 0x73, 0x65, 0x72, 0x5f,
    0x6d, 0x73, 0x67, 0x18, 0xa0, 0x8d, 0x06, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x07, 0x75, 0x73, 0x65,
    0x72, 0x4d, 0x73, 0x67, 0x42, 0x09, 0x0a, 0x07, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x42,
    0x07, 0x0a, 0x05, 0x72, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x2c, 0x0a, 0x07, 0x4d, 0x65, 0x6d, 0x62,
    0x65, 0x72, 0x73, 0x12, 0x21, 0x0a, 0x07, 0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x18, 0x01,
    0x20, 0x03, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x4d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x52, 0x07, 0x6d,
    0x65, 0x6d, 0x62, 0x65, 0x72, 0x73, 0x22, 0x43, 0x0a, 0x06, 0x4d, 0x65, 0x6d, 0x62, 0x65, 0x72,
    0x12, 0x1b, 0x0a, 0x04, 0x6e, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x07,
    0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x52, 0x04, 0x6e, 0x6f, 0x64, 0x65, 0x12, 0x1c, 0x0a,
    0x09, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08,
    0x52, 0x09, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x65, 0x64, 0x22, 0x2c, 0x0a, 0x07, 0x4d,
    0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x12, 0x21, 0x0a, 0x07, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63,
    0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63,
    0x52, 0x07, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x22, 0x80, 0x01, 0x0a, 0x06, 0x4d, 0x65,
    0x74, 0x72, 0x69, 0x63, 0x12, 0x16, 0x0a, 0x05, 0x67, 0x61, 0x75, 0x67, 0x65, 0x18, 0x01, 0x20,
    0x01, 0x28, 0x03, 0x48, 0x00, 0x52, 0x05, 0x67, 0x61, 0x75, 0x67, 0x65, 0x12, 0x1a, 0x0a, 0x07,
    0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x48, 0x00, 0x52,
    0x07, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x65, 0x72, 0x12, 0x38, 0x0a, 0x17, 0x76, 0x32, 0x5f, 0x73,
    0x65, 0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x5f, 0x68, 0x69, 0x73, 0x74, 0x6f, 0x67,
    0x72, 0x61, 0x6d, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x48, 0x00, 0x52, 0x15, 0x76, 0x32, 0x53,
    0x65, 0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x48, 0x69, 0x73, 0x74, 0x6f, 0x67, 0x72,
    0x61, 0x6d, 0x42, 0x08, 0x0a, 0x06, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x22, 0x96, 0x01, 0x0a,
    0x10, 0x43, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x4d, 0x73,
    0x67, 0x12, 0x24, 0x0a, 0x05, 0x6f, 0x72, 0x73, 0x65, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
    0x32, 0x0c, 0x2e, 0x4d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x4f, 0x52, 0x53, 0x65, 0x74, 0x48, 0x00,
    0x52, 0x05, 0x6f, 0x72, 0x73, 0x65, 0x74, 0x12, 0x14, 0x0a, 0x04, 0x50, 0x69, 0x6e, 0x67, 0x18,
    0x02, 0x20, 0x01, 0x28, 0x08, 0x48, 0x00, 0x52, 0x04, 0x50, 0x69, 0x6e, 0x67, 0x12, 0x27, 0x0a,
    0x08, 0x65, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32,
    0x09, 0x2e, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x48, 0x00, 0x52, 0x08, 0x65, 0x6e,
    0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x12, 0x16, 0x0a, 0x05, 0x44, 0x65, 0x6c, 0x74, 0x61, 0x18,
    0x04, 0x20, 0x01, 0x28, 0x0c, 0x48, 0x00, 0x52, 0x05, 0x44, 0x65, 0x6c, 0x74, 0x61, 0x42, 0x05,
    0x0a, 0x03, 0x6d, 0x73, 0x67, 0x22, 0x40, 0x0a, 0x0b, 0x4d, 0x65, 0x6d, 0x62, 0x65, 0x72, 0x4f,
    0x52, 0x53, 0x65, 0x74, 0x12, 0x1b, 0x0a, 0x04, 0x66, 0x72, 0x6f, 0x6d, 0x18, 0x01, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x07, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x52, 0x04, 0x66, 0x72, 0x6f,
    0x6d, 0x12, 0x14, 0x0a, 0x05, 0x6f, 0x72, 0x73, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c,
    0x52, 0x05, 0x6f, 0x72, 0x73, 0x65, 0x74, 0x4a, 0xa5, 0x18, 0x0a, 0x06, 0x12, 0x04, 0x00, 0x00,
    0x57, 0x01, 0x0a, 0x08, 0x0a, 0x01, 0x0c, 0x12, 0x03, 0x00, 0x00, 0x12, 0x0a, 0x0a, 0x0a, 0x02,
    0x04, 0x00, 0x12, 0x04, 0x02, 0x00, 0x05, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x00, 0x01, 0x12,
    0x03, 0x02, 0x08, 0x0e, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00, 0x02, 0x00, 0x12, 0x03, 0x03, 0x02,
    0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x04, 0x12, 0x03, 0x03, 0x02, 0x0a, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x05, 0x12, 0x03, 0x03, 0x0b, 0x11, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x00, 0x02, 0x00, 0x01, 0x12, 0x03, 0x03, 0x12, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x00, 0x02, 0x00, 0x03, 0x12, 0x03, 0x03, 0x19, 0x1a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00, 0x02,
    0x01, 0x12, 0x03, 0x04, 0x02, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01, 0x04, 0x12,
    0x03, 0x04, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01, 0x05, 0x12, 0x03, 0x04,
    0x0b, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01, 0x01, 0x12, 0x03, 0x04, 0x12, 0x16,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01, 0x03, 0x12, 0x03, 0x04, 0x19, 0x1a, 0x0a, 0x0a,
    0x0a, 0x02, 0x04, 0x01, 0x12, 0x04, 0x07, 0x00, 0x09, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x01,
    0x01, 0x12, 0x03, 0x07, 0x08, 0x0c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x01, 0x02, 0x00, 0x12, 0x03,
    0x08, 0x02, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x04, 0x12, 0x03, 0x08, 0x02,
    0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x06, 0x12, 0x03, 0x08, 0x0b, 0x0e, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x01, 0x12, 0x03, 0x08, 0x0f, 0x13, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x01, 0x02, 0x00, 0x03, 0x12, 0x03, 0x08, 0x16, 0x17, 0x0a, 0x0a, 0x0a, 0x02, 0x04,
    0x02, 0x12, 0x04, 0x0b, 0x00, 0x0f, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x02, 0x01, 0x12, 0x03,
    0x0b, 0x08, 0x0b, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x02, 0x02, 0x00, 0x12, 0x03, 0x0c, 0x02, 0x1b,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x04, 0x12, 0x03, 0x0c, 0x02, 0x0a, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x05, 0x12, 0x03, 0x0c, 0x0b, 0x11, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x02, 0x02, 0x00, 0x01, 0x12, 0x03, 0x0c, 0x12, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02,
    0x02, 0x00, 0x03, 0x12, 0x03, 0x0c, 0x19, 0x1a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x02, 0x02, 0x01,
    0x12, 0x03, 0x0d, 0x02, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x04, 0x12, 0x03,
    0x0d, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x05, 0x12, 0x03, 0x0d, 0x0b,
    0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x01, 0x12, 0x03, 0x0d, 0x12, 0x17, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x03, 0x12, 0x03, 0x0d, 0x1a, 0x1b, 0x0a, 0x0b, 0x0a,
    0x04, 0x04, 0x02, 0x02, 0x02, 0x12, 0x03, 0x0e, 0x02, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02,
    0x02, 0x02, 0x04, 0x12, 0x03, 0x0e, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x02,
    0x06, 0x12, 0x03, 0x0e, 0x0b, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x02, 0x01, 0x12,
    0x03, 0x0e, 0x12, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x02, 0x03, 0x12, 0x03, 0x0e,
    0x19, 0x1a, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x03, 0x12, 0x04, 0x11, 0x00, 0x15, 0x01, 0x0a, 0x0a,
    0x0a, 0x03, 0x04, 0x03, 0x01, 0x12, 0x03, 0x11, 0x08, 0x15, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03,
    0x02, 0x00, 0x12, 0x03, 0x12, 0x02, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x04,
    0x12, 0x03, 0x12, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x06, 0x12, 0x03,
    0x12, 0x0b, 0x0e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x01, 0x12, 0x03, 0x12, 0x0f,
    0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x03, 0x12, 0x03, 0x12, 0x15, 0x16, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02, 0x01, 0x12, 0x03, 0x13, 0x02, 0x1d, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x03, 0x02, 0x01, 0x04, 0x12, 0x03, 0x13, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03,
    0x02, 0x01, 0x05, 0x12, 0x03, 0x13, 0x0b, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x01,
    0x01, 0x12, 0x03, 0x13, 0x12, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x01, 0x03, 0x12,
    0x03, 0x13, 0x1b, 0x1c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02, 0x02, 0x12, 0x03, 0x14, 0x02,
    0x1e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x02, 0x04, 0x12, 0x03, 0x14, 0x02, 0x0a, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x02, 0x05, 0x12, 0x03, 0x14, 0x0b, 0x11, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x03, 0x02, 0x02, 0x01, 0x12, 0x03, 0x14, 0x12, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x03, 0x02, 0x02, 0x03, 0x12, 0x03, 0x14, 0x1c, 0x1d, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x04, 0x12,
    0x04, 0x17, 0x00, 0x1c, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x04, 0x01, 0x12, 0x03, 0x17, 0x08,
    0x10, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x04, 0x02, 0x00, 0x12, 0x03, 0x18, 0x02, 0x16, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x04, 0x12, 0x03, 0x18, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x04, 0x02, 0x00, 0x06, 0x12, 0x03, 0x18, 0x0b, 0x0e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04,
    0x02, 0x00, 0x01, 0x12, 0x03, 0x18, 0x0f, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00,
    0x03, 0x12, 0x03, 0x18, 0x14, 0x15, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x04, 0x02, 0x01, 0x12, 0x03,
    0x19, 0x02, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x01, 0x04, 0x12, 0x03, 0x19, 0x02,
    0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x01, 0x06, 0x12, 0x03, 0x19, 0x0b, 0x0e, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x01, 0x01, 0x12, 0x03, 0x19, 0x0f, 0x13, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x04, 0x02, 0x01, 0x03, 0x12, 0x03, 0x19, 0x16, 0x17, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x04, 0x02, 0x02, 0x12, 0x03, 0x1a, 0x02, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x02,
    0x04, 0x12, 0x03, 0x1a, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x02, 0x06, 0x12,
    0x03, 0x1a, 0x0b, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x02, 0x01, 0x12, 0x03, 0x1a,
    0x19, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x02, 0x03, 0x12, 0x03, 0x1a, 0x1f, 0x20,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x04, 0x02, 0x03, 0x12, 0x03, 0x1b, 0x02, 0x19, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x04, 0x02, 0x03, 0x04, 0x12, 0x03, 0x1b, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x04, 0x02, 0x03, 0x06, 0x12, 0x03, 0x1b, 0x0b, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02,
    0x03, 0x01, 0x12, 0x03, 0x1b, 0x11, 0x14, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x03, 0x03,
    0x12, 0x03, 0x1b, 0x17, 0x18, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x05, 0x12, 0x04, 0x1e, 0x00, 0x31,
    0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x05, 0x01, 0x12, 0x03, 0x1e, 0x08, 0x0d, 0x0a, 0x0c, 0x0a,
    0x04, 0x04, 0x05, 0x08, 0x00, 0x12, 0x04, 0x1f, 0x02, 0x26, 0x03, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x05, 0x08, 0x00, 0x01, 0x12, 0x03, 0x1f, 0x08, 0x0f, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x05, 0x02,
    0x00, 0x12, 0x03, 0x20, 0x08, 0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x00, 0x05, 0x12,
    0x03, 0x20, 0x08, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x00, 0x01, 0x12, 0x03, 0x20,
    0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x00, 0x03, 0x12, 0x03, 0x20, 0x1b, 0x1c,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x05, 0x02, 0x01, 0x12, 0x03, 0x21, 0x08, 0x1f, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x05, 0x02, 0x01, 0x05, 0x12, 0x03, 0x21, 0x08, 0x0e, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x05, 0x02, 0x01, 0x01, 0x12, 0x03, 0x21, 0x0f, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02,
    0x01, 0x03, 0x12, 0x03, 0x21, 0x1d, 0x1e, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x05, 0x02, 0x02, 0x12,
    0x03, 0x22, 0x08, 0x1e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x02, 0x05, 0x12, 0x03, 0x22,
    0x08, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x02, 0x01, 0x12, 0x03, 0x22, 0x0d, 0x19,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x02, 0x03, 0x12, 0x03, 0x22, 0x1c, 0x1d, 0x0a, 0x0b,
    0x0a, 0x04, 0x04, 0x05, 0x02, 0x03, 0x12, 0x03, 0x23, 0x08, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x05, 0x02, 0x03, 0x05, 0x12, 0x03, 0x23, 0x08, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02,
    0x03, 0x01, 0x12, 0x03, 0x23, 0x0d, 0x15, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x03, 0x03,
    0x12, 0x03, 0x23, 0x18, 0x19, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x05, 0x02, 0x04, 0x12, 0x03, 0x24,
    0x08, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x04, 0x06, 0x12, 0x03, 0x24, 0x08, 0x0e,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x04, 0x01, 0x12, 0x03, 0x24, 0x0f, 0x1c, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x05, 0x02, 0x04, 0x03, 0x12, 0x03, 0x24, 0x1f, 0x20, 0x0a, 0x0b, 0x0a, 0x04,
    0x04, 0x05, 0x02, 0x05, 0x12, 0x03, 0x25, 0x08, 0x20, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02,
    0x05, 0x06, 0x12, 0x03, 0x25, 0x08, 0x0e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x05, 0x01,
    0x12, 0x03, 0x25, 0x0f, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x05, 0x03, 0x12, 0x03,
    0x25, 0x1e, 0x1f, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x05, 0x08, 0x01, 0x12, 0x04, 0x27, 0x02, 0x2d,
    0x03, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x08, 0x01, 0x01, 0x12, 0x03, 0x27, 0x08, 0x0d, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x05, 0x02, 0x06, 0x12, 0x03, 0x28, 0x04, 0x1c, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x05, 0x02, 0x06, 0x06, 0x12, 0x03, 0x28, 0x04, 0x0b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05,
    0x02, 0x06, 0x01, 0x12, 0x03, 0x28, 0x0c, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x06,
    0x03, 0x12, 0x03, 0x28, 0x16, 0x1b, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x05, 0x02, 0x07, 0x12, 0x03,
    0x29, 0x04, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x07, 0x05, 0x12, 0x03, 0x29, 0x04,
    0x08, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x07, 0x01, 0x12, 0x03, 0x29, 0x09, 0x10, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x07, 0x03, 0x12, 0x03, 0x29, 0x13, 0x18, 0x0a, 0x0b, 0x0a,
    0x04, 0x04, 0x05, 0x02, 0x08, 0x12, 0x03, 0x2a, 0x04, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05,
    0x02, 0x08, 0x06, 0x12, 0x03, 0x2a, 0x04, 0x08, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x08,
    0x01, 0x12, 0x03, 0x2a, 0x09, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x08, 0x03, 0x12,
    0x03, 0x2a, 0x15, 0x1a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x05, 0x02, 0x09, 0x12, 0x03, 0x2b, 0x04,
    0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x09, 0x06, 0x12, 0x03, 0x2b, 0x04, 0x08, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x09, 0x01, 0x12, 0x03, 0x2b, 0x09, 0x11, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x05, 0x02, 0x09, 0x03, 0x12, 0x03, 0x2b, 0x14, 0x19, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x05, 0x02, 0x0a, 0x12, 0x03, 0x2c, 0x04, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x0a,
    0x06, 0x12, 0x03, 0x2c, 0x04, 0x0b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x0a, 0x01, 0x12,
    0x03, 0x2c, 0x0c, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x0a, 0x03, 0x12, 0x03, 0x2c,
    0x16, 0x1b, 0x0a, 0x5c, 0x0a, 0x04, 0x04, 0x05, 0x02, 0x0b, 0x12, 0x03, 0x30, 0x02, 0x23, 0x1a,
    0x4f, 0x20, 0x72, 0x75, 0x73, 0x74, 0x20, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x20,
    0x64, 0x6f, 0x65, 0x73, 0x6e, 0x27, 0x74, 0x20, 0x73, 0x75, 0x70, 0x70, 0x6f, 0x72, 0x74, 0x20,
    0x65, 0x78, 0x74, 0x65, 0x6e, 0x73, 0x69, 0x6f, 0x6e, 0x73, 0x20, 0x79, 0x65, 0x74, 0x2c, 0x20,
    0x73, 0x6f, 0x20, 0x77, 0x65, 0x20, 0x68, 0x61, 0x63, 0x6b, 0x20, 0x61, 0x72, 0x6f, 0x75, 0x6e,
    0x64, 0x20, 0x69, 0x74, 0x20, 0x76, 0x69, 0x61, 0x20, 0x62, 0x79, 0x74, 0x65, 0x73, 0x2e, 0x0a,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05, 0x02, 0x0b, 0x04, 0x12, 0x03, 0x30, 0x02, 0x0a, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x05, 0x02, 0x0b, 0x05, 0x12, 0x03, 0x30, 0x0b, 0x10, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x05, 0x02, 0x0b, 0x01, 0x12, 0x03, 0x30, 0x11, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x05,
    0x02, 0x0b, 0x03, 0x12, 0x03, 0x30, 0x1c, 0x22, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x06, 0x12, 0x04,
    0x33, 0x00, 0x35, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x06, 0x01, 0x12, 0x03, 0x33, 0x08, 0x0f,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x06, 0x02, 0x00, 0x12, 0x03, 0x34, 0x02, 0x1e, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x06, 0x02, 0x00, 0x04, 0x12, 0x03, 0x34, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x06, 0x02, 0x00, 0x06, 0x12, 0x03, 0x34, 0x0b, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02,
    0x00, 0x01, 0x12, 0x03, 0x34, 0x12, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x00, 0x03,
    0x12, 0x03, 0x34, 0x1c, 0x1d, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x07, 0x12, 0x04, 0x37, 0x00, 0x3a,
    0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x07, 0x01, 0x12, 0x03, 0x37, 0x08, 0x0e, 0x0a, 0x0b, 0x0a,
    0x04, 0x04, 0x07, 0x02, 0x00, 0x12, 0x03, 0x38, 0x02, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x07,
    0x02, 0x00, 0x04, 0x12, 0x03, 0x38, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x07, 0x02, 0x00,
    0x06, 0x12, 0x03, 0x38, 0x0b, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x07, 0x02, 0x00, 0x01, 0x12,
    0x03, 0x38, 0x12, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x07, 0x02, 0x00, 0x03, 0x12, 0x03, 0x38,
    0x19, 0x1a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x07, 0x02, 0x01, 0x12, 0x03, 0x39, 0x02, 0x1e, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x07, 0x02, 0x01, 0x04, 0x12, 0x03, 0x39, 0x02, 0x0a, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x07, 0x02, 0x01, 0x05, 0x12, 0x03, 0x39, 0x0b, 0x0f, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x07, 0x02, 0x01, 0x01, 0x12, 0x03, 0x39, 0x10, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x07, 0x02,
    0x01, 0x03, 0x12, 0x03, 0x39, 0x1c, 0x1d, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x08, 0x12, 0x04, 0x3c,
    0x00, 0x3e, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x08, 0x01, 0x12, 0x03, 0x3c, 0x08, 0x0f, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x08, 0x02, 0x00, 0x12, 0x03, 0x3d, 0x02, 0x1e, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x08, 0x02, 0x00, 0x04, 0x12, 0x03, 0x3d, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08,
    0x02, 0x00, 0x06, 0x12, 0x03, 0x3d, 0x0b, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x00,
    0x01, 0x12, 0x03, 0x3d, 0x12, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x00, 0x03, 0x12,
    0x03, 0x3d, 0x1c, 0x1d, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x09, 0x12, 0x04, 0x40, 0x00, 0x46, 0x01,
    0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x09, 0x01, 0x12, 0x03, 0x40, 0x08, 0x0e, 0x0a, 0x0c, 0x0a, 0x04,
    0x04, 0x09, 0x08, 0x00, 0x12, 0x04, 0x41, 0x02, 0x45, 0x03, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09,
    0x08, 0x00, 0x01, 0x12, 0x03, 0x41, 0x08, 0x0e, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x09, 0x02, 0x00,
    0x12, 0x03, 0x42, 0x04, 0x14, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x00, 0x05, 0x12, 0x03,
    0x42, 0x04, 0x09, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x00, 0x01, 0x12, 0x03, 0x42, 0x0a,
    0x0f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x00, 0x03, 0x12, 0x03, 0x42, 0x12, 0x13, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x09, 0x02, 0x01, 0x12, 0x03, 0x43, 0x04, 0x17, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x09, 0x02, 0x01, 0x05, 0x12, 0x03, 0x43, 0x04, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09,
    0x02, 0x01, 0x01, 0x12, 0x03, 0x43, 0x0b, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x01,
    0x03, 0x12, 0x03, 0x43, 0x15, 0x16, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x09, 0x02, 0x02, 0x12, 0x03,
    0x44, 0x04, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x02, 0x05, 0x12, 0x03, 0x44, 0x04,
    0x09, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x02, 0x01, 0x12, 0x03, 0x44, 0x0a, 0x21, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x02, 0x03, 0x12, 0x03, 0x44, 0x24, 0x25, 0x0a, 0x62, 0x0a,
    0x02, 0x04, 0x0a, 0x12, 0x04, 0x49, 0x00, 0x51, 0x01, 0x1a, 0x56, 0x20, 0x54, 0x68, 0x69, 0x73,
    0x20, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x20, 0x69, 0x73, 0x20, 0x75, 0x73, 0x65, 0x64,
    0x20, 0x6f, 0x6e, 0x6c, 0x79, 0x20, 0x62, 0x79, 0x20, 0x74, 0x68, 0x65, 0x20, 0x63, 0x6c, 0x75,
    0x73, 0x74, 0x65, 0x72, 0x20, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x20, 0x49, 0x74, 0x20,
    0x77, 0x69, 0x6c, 0x6c, 0x20, 0x6e, 0x65, 0x76, 0x65, 0x72, 0x20, 0x67, 0x65, 0x74, 0x20, 0x73,
    0x65, 0x6e, 0x74, 0x20, 0x74, 0x6f, 0x20, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x65, 0x73,
    0x0a, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x0a, 0x01, 0x12, 0x03, 0x49, 0x08, 0x18, 0x0a, 0x0c, 0x0a,
    0x04, 0x04, 0x0a, 0x08, 0x00, 0x12, 0x04, 0x4a, 0x02, 0x50, 0x03, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0a, 0x08, 0x00, 0x01, 0x12, 0x03, 0x4a, 0x08, 0x0b, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0a, 0x02,
    0x00, 0x12, 0x03, 0x4b, 0x04, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x00, 0x06, 0x12,
    0x03, 0x4b, 0x04, 0x0f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x00, 0x01, 0x12, 0x03, 0x4b,
    0x10, 0x15, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x00, 0x03, 0x12, 0x03, 0x4b, 0x18, 0x19,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0a, 0x02, 0x01, 0x12, 0x03, 0x4c, 0x04, 0x12, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0a, 0x02, 0x01, 0x05, 0x12, 0x03, 0x4c, 0x04, 0x08, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0a, 0x02, 0x01, 0x01, 0x12, 0x03, 0x4c, 0x09, 0x0d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02,
    0x01, 0x03, 0x12, 0x03, 0x4c, 0x10, 0x11, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0a, 0x02, 0x02, 0x12,
    0x03, 0x4d, 0x04, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x02, 0x06, 0x12, 0x03, 0x4d,
    0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x02, 0x01, 0x12, 0x03, 0x4d, 0x0d, 0x15,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x02, 0x03, 0x12, 0x03, 0x4d, 0x18, 0x19, 0x0a, 0x57,
    0x0a, 0x04, 0x04, 0x0a, 0x02, 0x03, 0x12, 0x03, 0x4f, 0x04, 0x14, 0x1a, 0x4a, 0x20, 0x54, 0x68,
    0x65, 0x20, 0x61, 0x63, 0x74, 0x75, 0x61, 0x6c, 0x20, 0x64, 0x65, 0x6c, 0x74, 0x61, 0x20, 0x69,
    0x73, 0x20, 0x73, 0x65, 0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x20, 0x69, 0x6e, 0x20,
    0x6d, 0x73, 0x67, 0x70, 0x61, 0x63, 0x6b, 0x20, 0x75, 0x73, 0x69, 0x6e, 0x67, 0x20, 0x72, 0x75,
    0x73, 0x74, 0x63, 0x5f, 0x73, 0x65, 0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x20, 0x66, 0x6f,
    0x72, 0x20, 0x6e, 0x6f, 0x77, 0x2e, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x03, 0x05,
    0x12, 0x03, 0x4f, 0x04, 0x09, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x03, 0x01, 0x12, 0x03,
    0x4f, 0x0a, 0x0f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x03, 0x03, 0x12, 0x03, 0x4f, 0x12,
    0x13, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x0b, 0x12, 0x04, 0x53, 0x00, 0x57, 0x01, 0x0a, 0x0a, 0x0a,
    0x03, 0x04, 0x0b, 0x01, 0x12, 0x03, 0x53, 0x08, 0x13, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0b, 0x02,
    0x00, 0x12, 0x03, 0x54, 0x02, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x00, 0x04, 0x12,
    0x03, 0x54, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x00, 0x06, 0x12, 0x03, 0x54,
    0x0b, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x00, 0x01, 0x12, 0x03, 0x54, 0x12, 0x16,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x00, 0x03, 0x12, 0x03, 0x54, 0x19, 0x1a, 0x0a, 0x57,
    0x0a, 0x04, 0x04, 0x0b, 0x02, 0x01, 0x12, 0x03, 0x56, 0x02, 0x1b, 0x1a, 0x4a, 0x20, 0x54, 0x68,
    0x65, 0x20, 0x61, 0x63, 0x74, 0x75, 0x61, 0x6c, 0x20, 0x4f, 0x52, 0x53, 0x65, 0x74, 0x20, 0x69,
    0x73, 0x20, 0x73, 0x65, 0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x20, 0x69, 0x6e, 0x20,
    0x6d, 0x73, 0x67, 0x70, 0x61, 0x63, 0x6b, 0x20, 0x75, 0x73, 0x69, 0x6e, 0x67, 0x20, 0x72, 0x75,
    0x73, 0x74, 0x63, 0x5f, 0x73, 0x65, 0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x20, 0x66, 0x6f,
    0x72, 0x20, 0x6e, 0x6f, 0x77, 0x2e, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x01, 0x04,
    0x12, 0x03, 0x56, 0x02, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x01, 0x05, 0x12, 0x03,
    0x56, 0x0b, 0x10, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x01, 0x01, 0x12, 0x03, 0x56, 0x11,
    0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x01, 0x03, 0x12, 0x03, 0x56, 0x19, 0x1a,
];

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}