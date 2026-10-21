//! Admin client.
//!
//! The main object is the [`AdminClient`] struct.
//!
//! [`AdminClient`]: struct.AdminClient.html

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use futures_channel::oneshot;
use futures_util::future::{self, Either, FutureExt};
use futures_util::ready;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::client::{Client, ClientContext, DefaultClientContext, NativeQueue};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::error::{IsError, KafkaError, KafkaResult};
use crate::log::{trace, warn};
use crate::util::{
    cstr_to_opt_owned, cstr_to_owned, AsCArray, ErrBuf, IntoOpaque, KafkaDrop, NativePtr, Timeout,
};
use crate::TopicPartitionList;

//
// ********** ADMIN CLIENT **********
//

/// A client for the Kafka admin API.
///
/// `AdminClient` provides programmatic access to managing a Kafka cluster,
/// notably manipulating topics, partitions, and configuration paramaters.
pub struct AdminClient<C: ClientContext> {
    client: Client<C>,
    queue: Arc<NativeQueue>,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl<C: ClientContext> fmt::Debug for AdminClient<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("AdminClient");
        debug.field("has_handle", &self.handle.is_some());
        debug.field("stop_requested", &self.should_stop.load(Ordering::Relaxed));
        debug.finish()
    }
}

impl<C: ClientContext> AdminClient<C> {
    /// Creates new topics according to the provided `NewTopic` specifications.
    ///
    /// Note that while the API supports creating multiple topics at once, it
    /// is not transactional. Creation of some topics may succeed while others
    /// fail. Be sure to check the result of each individual operation.
    pub fn create_topics<'a, I>(
        &self,
        topics: I,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<TopicResult>>>
    where
        I: IntoIterator<Item = &'a NewTopic<'a>>,
    {
        match self.create_topics_inner(topics, opts) {
            Ok(rx) => Either::Left(CreateTopicsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn create_topics_inner<'a, I>(
        &self,
        topics: I,
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>>
    where
        I: IntoIterator<Item = &'a NewTopic<'a>>,
    {
        let mut native_topics = Vec::new();
        let mut err_buf = ErrBuf::new();
        for t in topics {
            native_topics.push(t.to_native(&mut err_buf)?);
        }
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_CreateTopics(
                self.client.native_ptr(),
                native_topics.as_c_array(),
                native_topics.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            );
        }
        Ok(rx)
    }

    /// Deletes the named topics.
    ///
    /// Note that while the API supports deleting multiple topics at once, it is
    /// not transactional. Deletion of some topics may succeed while others
    /// fail. Be sure to check the result of each individual operation.
    pub fn delete_topics(
        &self,
        topic_names: &[&str],
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<TopicResult>>> {
        match self.delete_topics_inner(topic_names, opts) {
            Ok(rx) => Either::Left(DeleteTopicsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn delete_topics_inner(
        &self,
        topic_names: &[&str],
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>> {
        let mut native_topics = Vec::new();
        let mut err_buf = ErrBuf::new();
        for tn in topic_names {
            let tn_c = CString::new(*tn)?;
            let native_topic = unsafe {
                NativeDeleteTopic::from_ptr(rdsys::rd_kafka_DeleteTopic_new(tn_c.as_ptr())).unwrap()
            };
            native_topics.push(native_topic);
        }
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_DeleteTopics(
                self.client.native_ptr(),
                native_topics.as_c_array(),
                native_topics.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            );
        }
        Ok(rx)
    }

    /// Deletes the named groups.
    pub fn delete_groups(
        &self,
        group_names: &[&str],
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<GroupResult>>> {
        match self.delete_groups_inner(group_names, opts) {
            Ok(rx) => Either::Left(DeleteGroupsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn delete_groups_inner(
        &self,
        group_names: &[&str],
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>> {
        let mut native_groups = Vec::new();
        let mut err_buf = ErrBuf::new();
        for gn in group_names {
            let gn_t = CString::new(*gn)?;
            let native_group = unsafe {
                NativeDeleteGroup::from_ptr(rdsys::rd_kafka_DeleteGroup_new(gn_t.as_ptr())).unwrap()
            };
            native_groups.push(native_group);
        }
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;

        unsafe {
            rdsys::rd_kafka_DeleteGroups(
                self.client.native_ptr(),
                native_groups.as_c_array(),
                native_groups.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            )
        }
        Ok(rx)
    }

    /// Describes the named consumer groups.
    ///
    /// Unlike [`Client::fetch_group_list`], which only knows about classic
    /// groups, this also describes the groups of the consumer protocol
    /// introduced by KIP-848: librdkafka sends a ConsumerGroupDescribe request
    /// and falls back to DescribeGroups for classic groups.
    ///
    /// Groups whose protocol type is neither empty nor `consumer` (for
    /// example Kafka Connect groups) are reported as errors with
    /// [`RDKafkaErrorCode::InvalidArgument`].
    pub fn describe_consumer_groups(
        &self,
        group_names: &[&str],
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<ConsumerGroupDescriptionResult>>> {
        match self.describe_consumer_groups_inner(group_names, opts) {
            Ok(rx) => Either::Left(DescribeConsumerGroupsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn describe_consumer_groups_inner(
        &self,
        group_names: &[&str],
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>> {
        let names = group_names
            .iter()
            .map(|gn| CString::new(*gn))
            .collect::<Result<Vec<_>, _>>()?;
        let mut name_ptrs: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
        let mut err_buf = ErrBuf::new();
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        // librdkafka copies the names, so `names` only needs to outlive the call.
        unsafe {
            rdsys::rd_kafka_DescribeConsumerGroups(
                self.client.native_ptr(),
                name_ptrs.as_mut_ptr(),
                name_ptrs.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            )
        }
        Ok(rx)
    }

    /// Creates the given ACL bindings.
    ///
    /// The result has one entry per binding, in the same order.
    pub fn create_acls(
        &self,
        acls: &[AclBinding],
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<AclResult>>> {
        match self.create_acls_inner(acls, opts) {
            Ok(rx) => Either::Left(CreateAclsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn create_acls_inner(
        &self,
        acls: &[AclBinding],
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>> {
        let mut err_buf = ErrBuf::new();
        let native_acls = acls
            .iter()
            .map(|acl| acl.to_native(&mut err_buf))
            .collect::<KafkaResult<Vec<_>>>()?;
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        // librdkafka copies the bindings, which are destroyed on return.
        unsafe {
            rdsys::rd_kafka_CreateAcls(
                self.client.native_ptr(),
                native_acls.as_c_array(),
                native_acls.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            )
        }
        Ok(rx)
    }

    /// Describes the ACL bindings that match the given filter.
    pub fn describe_acls(
        &self,
        filter: &AclBindingFilter,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<AclBinding>>> {
        match self.describe_acls_inner(filter, opts) {
            Ok(rx) => Either::Left(DescribeAclsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn describe_acls_inner(
        &self,
        filter: &AclBindingFilter,
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>> {
        let mut err_buf = ErrBuf::new();
        let native_filter = filter.to_native(&mut err_buf)?;
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_DescribeAcls(
                self.client.native_ptr(),
                native_filter.ptr(),
                native_opts.ptr(),
                self.queue.ptr(),
            )
        }
        Ok(rx)
    }

    /// Deletes the ACL bindings that match the given filters.
    ///
    /// The result has one entry per filter, in the same order, with the
    /// bindings that the filter matched and deleted.
    pub fn delete_acls(
        &self,
        filters: &[AclBindingFilter],
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<DeleteAclsResult>>> {
        match self.delete_acls_inner(filters, opts) {
            Ok(rx) => Either::Left(DeleteAclsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn delete_acls_inner(
        &self,
        filters: &[AclBindingFilter],
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>> {
        let mut err_buf = ErrBuf::new();
        let native_filters = filters
            .iter()
            .map(|filter| filter.to_native(&mut err_buf))
            .collect::<KafkaResult<Vec<_>>>()?;
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_DeleteAcls(
                self.client.native_ptr(),
                native_filters.as_c_array(),
                native_filters.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            )
        }
        Ok(rx)
    }

    /// Adds additional partitions to existing topics according to the provided
    /// `NewPartitions` specifications.
    ///
    /// Note that while the API supports creating partitions for multiple topics
    /// at once, it is not transactional. Creation of partitions for some topics
    /// may succeed while others fail. Be sure to check the result of each
    /// individual operation.
    pub fn create_partitions<'a, I>(
        &self,
        partitions: I,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<TopicResult>>>
    where
        I: IntoIterator<Item = &'a NewPartitions<'a>>,
    {
        match self.create_partitions_inner(partitions, opts) {
            Ok(rx) => Either::Left(CreatePartitionsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn create_partitions_inner<'a, I>(
        &self,
        partitions: I,
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>>
    where
        I: IntoIterator<Item = &'a NewPartitions<'a>>,
    {
        let mut native_partitions = Vec::new();
        let mut err_buf = ErrBuf::new();
        for p in partitions {
            native_partitions.push(p.to_native(&mut err_buf)?);
        }
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_CreatePartitions(
                self.client.native_ptr(),
                native_partitions.as_c_array(),
                native_partitions.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            );
        }
        Ok(rx)
    }

    /// Deletes records from a topic.
    ///
    /// The provided `offsets` is a topic partition list specifying which
    /// records to delete from a list of topic partitions. For each entry in the
    /// list, the messages at offsets before the specified offsets (exclusive)
    /// in the specified partition will be deleted. Use offset [`crate::Offset::End`]
    /// to delete all records in the partition.
    ///
    /// Returns a topic partition list describing the result of the deletion. If
    /// the operation succeeded for a partition, the offset for that partition
    /// will be set to the post-deletion low-water mark for that partition. If
    /// the operation failed for a partition, there will be an error for that
    /// partition's entry in the list.
    pub fn delete_records(
        &self,
        offsets: &TopicPartitionList,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<TopicPartitionList>> {
        match self.delete_records_inner(offsets, opts) {
            Ok(rx) => Either::Left(DeleteRecordsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn delete_records_inner(
        &self,
        offsets: &TopicPartitionList,
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>> {
        let mut err_buf = ErrBuf::new();
        let delete_records = unsafe {
            NativeDeleteRecords::from_ptr(rdsys::rd_kafka_DeleteRecords_new(offsets.ptr()))
        }
        .ok_or_else(|| KafkaError::AdminOpCreation(err_buf.to_string()))?;
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_DeleteRecords(
                self.client.native_ptr(),
                &mut delete_records.ptr(),
                1,
                native_opts.ptr(),
                self.queue.ptr(),
            );
        }
        Ok(rx)
    }

    /// Retrieves the configuration parameters for the specified resources.
    ///
    /// Note that while the API supports describing multiple configurations at
    /// once, it is not transactional. There is no guarantee that you will see
    /// a consistent snapshot of the configuration across different resources.
    pub fn describe_configs<'a, I>(
        &self,
        configs: I,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<ConfigResourceResult>>>
    where
        I: IntoIterator<Item = &'a ResourceSpecifier<'a>>,
    {
        match self.describe_configs_inner(configs, opts) {
            Ok(rx) => Either::Left(DescribeConfigsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn describe_configs_inner<'a, I>(
        &self,
        configs: I,
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>>
    where
        I: IntoIterator<Item = &'a ResourceSpecifier<'a>>,
    {
        let mut native_configs = Vec::new();
        let mut err_buf = ErrBuf::new();
        for c in configs {
            let (name, typ) = match c {
                ResourceSpecifier::Topic(name) => (
                    CString::new(*name)?,
                    RDKafkaResourceType::RD_KAFKA_RESOURCE_TOPIC,
                ),
                ResourceSpecifier::Group(name) => (
                    CString::new(*name)?,
                    RDKafkaResourceType::RD_KAFKA_RESOURCE_GROUP,
                ),
                ResourceSpecifier::Broker(id) => (
                    CString::new(format!("{}", id))?,
                    RDKafkaResourceType::RD_KAFKA_RESOURCE_BROKER,
                ),
            };
            native_configs.push(unsafe {
                NativeConfigResource::from_ptr(rdsys::rd_kafka_ConfigResource_new(
                    typ,
                    name.as_ptr(),
                ))
                .unwrap()
            });
        }
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_DescribeConfigs(
                self.client.native_ptr(),
                native_configs.as_c_array(),
                native_configs.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            );
        }
        Ok(rx)
    }

    /// Sets configuration parameters for the specified resources,
    /// resetting unspecified parameters to their default values.
    ///
    /// Note that while the API supports altering multiple resources at once, it
    /// is not transactional. Alteration of some resources may succeed while
    /// others fail. Be sure to check the result of each individual operation.
    pub fn alter_configs<'a, I>(
        &self,
        configs: I,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<AlterConfigsResult>>>
    where
        I: IntoIterator<Item = &'a AlterConfig<'a>>,
    {
        match self.alter_configs_inner(configs, opts) {
            Ok(rx) => Either::Left(AlterConfigsFuture { rx }),
            Err(err) => Either::Right(future::err(err)),
        }
    }

    fn alter_configs_inner<'a, I>(
        &self,
        configs: I,
        opts: &AdminOptions,
    ) -> KafkaResult<oneshot::Receiver<NativeEvent>>
    where
        I: IntoIterator<Item = &'a AlterConfig<'a>>,
    {
        let mut native_configs = Vec::new();
        let mut err_buf = ErrBuf::new();
        for c in configs {
            native_configs.push(c.to_native(&mut err_buf)?);
        }
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_AlterConfigs(
                self.client.native_ptr(),
                native_configs.as_c_array(),
                native_configs.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            );
        }
        Ok(rx)
    }

    /// Returns the client underlying this admin client.
    pub fn inner(&self) -> &Client<C> {
        &self.client
    }
}

impl FromClientConfig for AdminClient<DefaultClientContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<AdminClient<DefaultClientContext>> {
        AdminClient::from_config_and_context(config, DefaultClientContext)
    }
}

impl<C: ClientContext> FromClientConfigAndContext<C> for AdminClient<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<AdminClient<C>> {
        let native_config = config.create_native_config()?;
        // librdkafka only provides consumer and producer types. We follow the
        // example of the Python bindings in choosing to pretend to be a
        // producer, as producer clients are allegedly more lightweight. [0]
        //
        // [0]: https://github.com/confluentinc/confluent-kafka-python/blob/bfb07dfbca47c256c840aaace83d3fe26c587360/confluent_kafka/src/Admin.c#L1492-L1493
        let client = Client::new(
            config,
            native_config,
            RDKafkaType::RD_KAFKA_PRODUCER,
            context,
        )?;
        let queue = Arc::new(client.new_native_queue());
        let should_stop = Arc::new(AtomicBool::new(false));
        let handle = start_poll_thread(queue.clone(), should_stop.clone());
        Ok(AdminClient {
            client,
            queue,
            should_stop,
            handle: Some(handle),
        })
    }
}

impl<C: ClientContext> Drop for AdminClient<C> {
    fn drop(&mut self) {
        trace!("Stopping polling");
        self.should_stop.store(true, Ordering::Relaxed);
        trace!("Waiting for polling thread termination");
        match self.handle.take().unwrap().join() {
            Ok(()) => trace!("Polling stopped"),
            Err(e) => warn!("Failure while terminating thread: {:?}", e),
        };
    }
}

fn start_poll_thread(queue: Arc<NativeQueue>, should_stop: Arc<AtomicBool>) -> JoinHandle<()> {
    thread::Builder::new()
        .name("admin client polling thread".into())
        .spawn(move || {
            trace!("Admin polling thread loop started");
            loop {
                let event = queue.poll(Duration::from_millis(100));
                if event.is_null() {
                    if should_stop.load(Ordering::Relaxed) {
                        // We received nothing and the thread should stop, so
                        // break the loop.
                        break;
                    }
                    continue;
                }
                let event = unsafe { NativeEvent::from_ptr(event).unwrap() };
                let tx: Box<oneshot::Sender<NativeEvent>> =
                    unsafe { IntoOpaque::from_ptr(rdsys::rd_kafka_event_opaque(event.ptr())) };
                let _ = tx.send(event);
            }
            trace!("Admin polling thread loop terminated");
        })
        .expect("Failed to start polling thread")
}

pub(crate) type NativeEvent = NativePtr<RDKafkaEvent>;

unsafe impl KafkaDrop for RDKafkaEvent {
    const TYPE: &'static str = "event";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_event_destroy;
}

unsafe impl Send for NativeEvent {}
unsafe impl Sync for NativeEvent {}

impl NativePtr<RDKafkaEvent> {
    fn check_error(&self) -> KafkaResult<()> {
        let err = unsafe { rdsys::rd_kafka_event_error(self.ptr()) };
        if err.is_error() {
            Err(KafkaError::AdminOp(err.into()))
        } else {
            Ok(())
        }
    }
}

//
// ********** ADMIN OPTIONS **********
//

/// Options for an admin API request.
#[derive(Default)]
pub struct AdminOptions {
    request_timeout: Option<Timeout>,
    operation_timeout: Option<Timeout>,
    validate_only: bool,
    broker_id: Option<i32>,
}

impl AdminOptions {
    /// Creates a new `AdminOptions`.
    pub fn new() -> AdminOptions {
        AdminOptions::default()
    }

    /// Sets the overall request timeout, including broker lookup, request
    /// transmission, operation time on broker, and response.
    ///
    /// Defaults to the `socket.timeout.ms` configuration parameter.
    pub fn request_timeout<T: Into<Timeout>>(mut self, timeout: Option<T>) -> Self {
        self.request_timeout = timeout.map(Into::into);
        self
    }

    /// Sets the broker's operation timeout, such as the timeout for
    /// CreateTopics to complete the creation of topics on the controller before
    /// returning a result to the application.
    ///
    /// If unset (the default), the API calls will return immediately after
    /// triggering the operation.
    ///
    /// Only the CreateTopics, DeleteTopics, and CreatePartitions API calls
    /// respect this option.
    pub fn operation_timeout<T: Into<Timeout>>(mut self, timeout: Option<T>) -> Self {
        self.operation_timeout = timeout.map(Into::into);
        self
    }

    /// Tells the broker to only validate the request, without performing the
    /// requested operation.
    ///
    /// Defaults to false.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Override what broker the admin request will be sent to.
    ///
    /// By default, a reasonable broker will be selected automatically. See the
    /// librdkafka docs on `rd_kafka_AdminOptions_set_broker` for details.
    pub fn broker_id<T: Into<Option<i32>>>(mut self, broker_id: T) -> Self {
        self.broker_id = broker_id.into();
        self
    }

    fn to_native(
        &self,
        client: *mut RDKafka,
        err_buf: &mut ErrBuf,
    ) -> KafkaResult<(NativeAdminOptions, oneshot::Receiver<NativeEvent>)> {
        let native_opts = unsafe {
            NativeAdminOptions::from_ptr(rdsys::rd_kafka_AdminOptions_new(
                client,
                RDKafkaAdminOp::RD_KAFKA_ADMIN_OP_ANY,
            ))
            .unwrap()
        };

        if let Some(timeout) = self.request_timeout {
            let res = unsafe {
                rdsys::rd_kafka_AdminOptions_set_request_timeout(
                    native_opts.ptr(),
                    timeout.as_millis(),
                    err_buf.as_mut_ptr(),
                    err_buf.capacity(),
                )
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }

        if let Some(timeout) = self.operation_timeout {
            let res = unsafe {
                rdsys::rd_kafka_AdminOptions_set_operation_timeout(
                    native_opts.ptr(),
                    timeout.as_millis(),
                    err_buf.as_mut_ptr(),
                    err_buf.capacity(),
                )
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }

        if self.validate_only {
            let res = unsafe {
                rdsys::rd_kafka_AdminOptions_set_validate_only(
                    native_opts.ptr(),
                    1, // true
                    err_buf.as_mut_ptr(),
                    err_buf.capacity(),
                )
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }

        if let Some(broker_id) = self.broker_id {
            let res = unsafe {
                rdsys::rd_kafka_AdminOptions_set_broker(
                    native_opts.ptr(),
                    broker_id,
                    err_buf.as_mut_ptr(),
                    err_buf.capacity(),
                )
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }

        let (tx, rx) = oneshot::channel();
        let tx = Box::into_raw(Box::new(tx)) as *mut c_void;
        unsafe { rdsys::rd_kafka_AdminOptions_set_opaque(native_opts.ptr(), tx) };

        Ok((native_opts, rx))
    }
}

unsafe impl KafkaDrop for RDKafkaAdminOptions {
    const TYPE: &'static str = "admin options";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_AdminOptions_destroy;
}

type NativeAdminOptions = NativePtr<RDKafkaAdminOptions>;

fn check_rdkafka_invalid_arg(res: RDKafkaRespErr, err_buf: &ErrBuf) -> KafkaResult<()> {
    match res.into() {
        RDKafkaErrorCode::NoError => Ok(()),
        RDKafkaErrorCode::InvalidArgument => {
            let msg = if err_buf.len() == 0 {
                "invalid argument".into()
            } else {
                err_buf.to_string()
            };
            Err(KafkaError::AdminOpCreation(msg))
        }
        res => Err(KafkaError::AdminOpCreation(format!(
            "setting admin options returned unexpected error code {}",
            res
        ))),
    }
}

//
// ********** RESPONSE HANDLING **********
//

/// The result of an individual CreateTopic, DeleteTopic, or
/// CreatePartition operation.
pub type TopicResult = Result<String, (String, RDKafkaErrorCode)>;

fn build_topic_results(topics: *const *const RDKafkaTopicResult, n: usize) -> Vec<TopicResult> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let topic = unsafe { *topics.add(i) };
        let name = unsafe { cstr_to_owned(rdsys::rd_kafka_topic_result_name(topic)) };
        let err = unsafe { rdsys::rd_kafka_topic_result_error(topic) };
        if err.is_error() {
            out.push(Err((name, err.into())));
        } else {
            out.push(Ok(name));
        }
    }
    out
}

/// The result of a DeleteGroup operation.
pub type GroupResult = Result<String, (String, RDKafkaErrorCode)>;

fn build_group_results(groups: *const *const RDKafkaGroupResult, n: usize) -> Vec<GroupResult> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let group = unsafe { *groups.add(i) };
        let name = unsafe { cstr_to_owned(rdsys::rd_kafka_group_result_name(group)) };
        let err = unsafe {
            let err = rdsys::rd_kafka_group_result_error(group);
            rdsys::rd_kafka_error_code(err)
        };
        if err.is_error() {
            out.push(Err((name, err.into())));
        } else {
            out.push(Ok(name));
        }
    }
    out
}

//
// Create topic handling
//

/// Configuration for a CreateTopic operation.
#[derive(Debug)]
pub struct NewTopic<'a> {
    /// The name of the new topic.
    pub name: &'a str,
    /// The initial number of partitions.
    pub num_partitions: i32,
    /// The initial replication configuration.
    pub replication: TopicReplication<'a>,
    /// The initial configuration parameters for the topic.
    pub config: Vec<(&'a str, &'a str)>,
}

impl<'a> NewTopic<'a> {
    /// Creates a new `NewTopic`.
    pub fn new(
        name: &'a str,
        num_partitions: i32,
        replication: TopicReplication<'a>,
    ) -> NewTopic<'a> {
        NewTopic {
            name,
            num_partitions,
            replication,
            config: Vec::new(),
        }
    }

    /// Sets a new parameter in the initial topic configuration.
    pub fn set(mut self, key: &'a str, value: &'a str) -> NewTopic<'a> {
        self.config.push((key, value));
        self
    }

    fn to_native(&self, err_buf: &mut ErrBuf) -> KafkaResult<NativeNewTopic> {
        let name = CString::new(self.name)?;
        let repl = match self.replication {
            TopicReplication::Fixed(n) => n,
            TopicReplication::Variable(partitions) => {
                if partitions.len() as i32 != self.num_partitions {
                    return Err(KafkaError::AdminOpCreation(format!(
                        "replication configuration for topic '{}' assigns {} partition(s), \
                         which does not match the specified number of partitions ({})",
                        self.name,
                        partitions.len(),
                        self.num_partitions,
                    )));
                }
                -1
            }
        };
        // N.B.: we wrap topic immediately, so that it is destroyed via the
        // NativeNewTopic's Drop implementation if replica assignment or config
        // installation fails.
        let topic = unsafe {
            NativeNewTopic::from_ptr(rdsys::rd_kafka_NewTopic_new(
                name.as_ptr(),
                self.num_partitions,
                repl,
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            ))
        }
        .ok_or_else(|| KafkaError::AdminOpCreation(err_buf.to_string()))?;

        if let TopicReplication::Variable(assignment) = self.replication {
            for (partition_id, broker_ids) in assignment.iter().enumerate() {
                let res = unsafe {
                    rdsys::rd_kafka_NewTopic_set_replica_assignment(
                        topic.ptr(),
                        partition_id as i32,
                        broker_ids.as_ptr() as *mut i32,
                        broker_ids.len(),
                        err_buf.as_mut_ptr(),
                        err_buf.capacity(),
                    )
                };
                check_rdkafka_invalid_arg(res, err_buf)?;
            }
        }
        for (key, val) in &self.config {
            let key_c = CString::new(*key)?;
            let val_c = CString::new(*val)?;
            let res = unsafe {
                rdsys::rd_kafka_NewTopic_set_config(topic.ptr(), key_c.as_ptr(), val_c.as_ptr())
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }
        Ok(topic)
    }
}

/// An assignment of partitions to replicas.
///
/// Each element in the outer slice corresponds to the partition with that
/// index. The inner slice specifies the broker IDs to which replicas of that
/// partition should be assigned.
pub type PartitionAssignment<'a> = &'a [&'a [i32]];

/// Replication configuration for a new topic.
#[derive(Debug)]
pub enum TopicReplication<'a> {
    /// All partitions should use the same fixed replication factor.
    Fixed(i32),
    /// Each partition should use the replica assignment from
    /// `PartitionAssignment`.
    Variable(PartitionAssignment<'a>),
}

type NativeNewTopic = NativePtr<RDKafkaNewTopic>;

unsafe impl KafkaDrop for RDKafkaNewTopic {
    const TYPE: &'static str = "new topic";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_NewTopic_destroy;
}

struct CreateTopicsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for CreateTopicsFuture {
    type Output = KafkaResult<Vec<TopicResult>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_CreateTopics_result(event.ptr()) };
        if res.is_null() {
            let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
            return Poll::Ready(Err(KafkaError::AdminOpCreation(format!(
                "create topics request received response of incorrect type ({})",
                typ
            ))));
        }
        let mut n = 0;
        let topics = unsafe { rdsys::rd_kafka_CreateTopics_result_topics(res, &mut n) };
        Poll::Ready(Ok(build_topic_results(topics, n)))
    }
}

//
// Delete topic handling
//

type NativeDeleteTopic = NativePtr<RDKafkaDeleteTopic>;

unsafe impl KafkaDrop for RDKafkaDeleteTopic {
    const TYPE: &'static str = "delete topic";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_DeleteTopic_destroy;
}

struct DeleteTopicsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for DeleteTopicsFuture {
    type Output = KafkaResult<Vec<TopicResult>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_DeleteTopics_result(event.ptr()) };
        if res.is_null() {
            let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
            return Poll::Ready(Err(KafkaError::AdminOpCreation(format!(
                "delete topics request received response of incorrect type ({})",
                typ
            ))));
        }
        let mut n = 0;
        let topics = unsafe { rdsys::rd_kafka_DeleteTopics_result_topics(res, &mut n) };
        Poll::Ready(Ok(build_topic_results(topics, n)))
    }
}

//
// Delete group handling
//

type NativeDeleteGroup = NativePtr<RDKafkaDeleteGroup>;

unsafe impl KafkaDrop for RDKafkaDeleteGroup {
    const TYPE: &'static str = "delete group";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_DeleteGroup_destroy;
}

struct DeleteGroupsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for DeleteGroupsFuture {
    type Output = KafkaResult<Vec<GroupResult>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_DeleteGroups_result(event.ptr()) };
        if res.is_null() {
            let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
            return Poll::Ready(Err(KafkaError::AdminOpCreation(format!(
                "delete groups request received response of incorrect type ({})",
                typ
            ))));
        }
        let mut n = 0;
        let groups = unsafe { rdsys::rd_kafka_DeleteGroups_result_groups(res, &mut n) };
        Poll::Ready(Ok(build_group_results(groups, n)))
    }
}

//
// Describe consumer groups handling
//

/// The state of a consumer group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumerGroupState {
    /// The state is unknown, or not one librdkafka can name (for example the
    /// `Assigning` and `Reconciling` states of KIP-848 groups).
    Unknown,
    /// The group is preparing a rebalance.
    PreparingRebalance,
    /// The group is completing a rebalance.
    CompletingRebalance,
    /// The group is stable.
    Stable,
    /// The group does not exist or is being deleted.
    Dead,
    /// The group has no members.
    Empty,
}

impl ConsumerGroupState {
    fn from_native(state: rdsys::rd_kafka_consumer_group_state_t) -> Self {
        use rdsys::rd_kafka_consumer_group_state_t::*;
        match state {
            RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE => Self::PreparingRebalance,
            RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE => Self::CompletingRebalance,
            RD_KAFKA_CONSUMER_GROUP_STATE_STABLE => Self::Stable,
            RD_KAFKA_CONSUMER_GROUP_STATE_DEAD => Self::Dead,
            RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY => Self::Empty,
            _ => Self::Unknown,
        }
    }
}

impl fmt::Display for ConsumerGroupState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "Unknown",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
            Self::Dead => "Dead",
            Self::Empty => "Empty",
        })
    }
}

/// The protocol of a consumer group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConsumerGroupType {
    /// The type is unknown (for example, the broker is too old to say).
    Unknown,
    /// The consumer protocol introduced by KIP-848.
    Consumer,
    /// The classic protocol.
    Classic,
}

impl ConsumerGroupType {
    fn from_native(group_type: rdsys::rd_kafka_consumer_group_type_t) -> Self {
        use rdsys::rd_kafka_consumer_group_type_t::*;
        match group_type {
            RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER => Self::Consumer,
            RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC => Self::Classic,
            _ => Self::Unknown,
        }
    }
}

impl fmt::Display for ConsumerGroupType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "Unknown",
            Self::Consumer => "Consumer",
            Self::Classic => "Classic",
        })
    }
}

/// A broker, as returned by the admin API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// The broker ID.
    pub id: i32,
    /// The broker host.
    pub host: String,
    /// The broker port.
    pub port: u16,
    /// The broker rack, if any.
    pub rack: Option<String>,
}

impl Node {
    unsafe fn from_native(node: *const rdsys::rd_kafka_Node_t) -> Option<Node> {
        if node.is_null() {
            return None;
        }
        Some(Node {
            id: rdsys::rd_kafka_Node_id(node),
            host: cstr_to_owned(rdsys::rd_kafka_Node_host(node)),
            port: rdsys::rd_kafka_Node_port(node),
            rack: cstr_to_opt_owned(rdsys::rd_kafka_Node_rack(node)),
        })
    }
}

/// A member of a consumer group.
#[derive(Debug, Clone)]
pub struct MemberDescription {
    /// The client ID of the member.
    pub client_id: String,
    /// The group instance ID, for static members.
    pub group_instance_id: Option<String>,
    /// The member ID assigned by the group coordinator.
    pub consumer_id: String,
    /// The host the member connects from.
    pub host: String,
    /// The partitions assigned to the member.
    pub assignment: TopicPartitionList,
    /// The partitions the member is moving to (KIP-848 groups only).
    pub target_assignment: Option<TopicPartitionList>,
}

/// The description of a consumer group.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescription {
    /// The group ID.
    pub group_id: String,
    /// Whether the group is a simple consumer group (no protocol type).
    pub is_simple_consumer_group: bool,
    /// The partition assignor of the group.
    pub partition_assignor: String,
    /// The state of the group.
    pub state: ConsumerGroupState,
    /// The protocol of the group.
    pub group_type: ConsumerGroupType,
    /// The coordinator of the group, if known.
    pub coordinator: Option<Node>,
    /// The members of the group.
    pub members: Vec<MemberDescription>,
}

/// The result of an individual DescribeConsumerGroups operation: the
/// description, or the group ID with the error code and message.
pub type ConsumerGroupDescriptionResult =
    Result<ConsumerGroupDescription, (String, RDKafkaErrorCode, String)>;

/// A copy of a topic partition list owned by librdkafka.
unsafe fn copy_partition_list(tpl: *const RDKafkaTopicPartitionList) -> Option<TopicPartitionList> {
    if tpl.is_null() {
        None
    } else {
        Some(TopicPartitionList::from_ptr(
            rdsys::rd_kafka_topic_partition_list_copy(tpl),
        ))
    }
}

/// A copy of a member assignment owned by librdkafka.
unsafe fn copy_assignment(
    assignment: *const rdsys::rd_kafka_MemberAssignment_t,
) -> Option<TopicPartitionList> {
    if assignment.is_null() {
        None
    } else {
        copy_partition_list(rdsys::rd_kafka_MemberAssignment_partitions(assignment))
    }
}

unsafe fn build_member_description(
    member: *const rdsys::rd_kafka_MemberDescription_t,
) -> MemberDescription {
    MemberDescription {
        client_id: cstr_to_owned(rdsys::rd_kafka_MemberDescription_client_id(member)),
        group_instance_id: cstr_to_opt_owned(rdsys::rd_kafka_MemberDescription_group_instance_id(
            member,
        )),
        consumer_id: cstr_to_owned(rdsys::rd_kafka_MemberDescription_consumer_id(member)),
        host: cstr_to_owned(rdsys::rd_kafka_MemberDescription_host(member)),
        assignment: copy_assignment(rdsys::rd_kafka_MemberDescription_assignment(member))
            .unwrap_or_default(),
        target_assignment: copy_assignment(rdsys::rd_kafka_MemberDescription_target_assignment(
            member,
        )),
    }
}

unsafe fn build_consumer_group_description(
    group: *const rdsys::rd_kafka_ConsumerGroupDescription_t,
) -> ConsumerGroupDescriptionResult {
    let group_id = cstr_to_owned(rdsys::rd_kafka_ConsumerGroupDescription_group_id(group));
    if let Some((code, message)) =
        error_with_message(rdsys::rd_kafka_ConsumerGroupDescription_error(group))
    {
        return Err((group_id, code, message));
    }
    let member_count = rdsys::rd_kafka_ConsumerGroupDescription_member_count(group);
    let members = (0..member_count)
        .map(|i| {
            build_member_description(rdsys::rd_kafka_ConsumerGroupDescription_member(group, i))
        })
        .collect();
    Ok(ConsumerGroupDescription {
        group_id,
        is_simple_consumer_group: rdsys::rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(
            group,
        ) != 0,
        partition_assignor: cstr_to_opt_owned(
            rdsys::rd_kafka_ConsumerGroupDescription_partition_assignor(group),
        )
        .unwrap_or_default(),
        state: ConsumerGroupState::from_native(rdsys::rd_kafka_ConsumerGroupDescription_state(
            group,
        )),
        group_type: ConsumerGroupType::from_native(rdsys::rd_kafka_ConsumerGroupDescription_type(
            group,
        )),
        coordinator: Node::from_native(rdsys::rd_kafka_ConsumerGroupDescription_coordinator(group)),
        members,
    })
}

struct DescribeConsumerGroupsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for DescribeConsumerGroupsFuture {
    type Output = KafkaResult<Vec<ConsumerGroupDescriptionResult>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_DescribeConsumerGroups_result(event.ptr()) };
        if res.is_null() {
            let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
            return Poll::Ready(Err(KafkaError::AdminOpCreation(format!(
                "describe consumer groups request received response of incorrect type ({})",
                typ
            ))));
        }
        let mut n = 0;
        let groups = unsafe { rdsys::rd_kafka_DescribeConsumerGroups_result_groups(res, &mut n) };
        let out = (0..n)
            .map(|i| unsafe { build_consumer_group_description(*groups.add(i)) })
            .collect();
        Poll::Ready(Ok(out))
    }
}

//
// ACL handling
//

/// Declares an enum that mirrors a librdkafka enum, with conversions in
/// both directions and the librdkafka name of each value.
macro_rules! acl_enum {
    (
        $(#[$meta:meta])*
        $name:ident, $native:ty, $name_fn:path, $unknown:ident {
            $($(#[$vmeta:meta])* $variant:ident = $native_variant:ident,)*
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $($(#[$vmeta])* $variant,)*
        }

        impl $name {
            fn to_native(self) -> $native {
                match self {
                    $(Self::$variant => <$native>::$native_variant,)*
                }
            }

            fn from_native(native: $native) -> Self {
                match native {
                    $(<$native>::$native_variant => Self::$variant,)*
                    #[allow(unreachable_patterns)]
                    _ => Self::$unknown,
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let name = unsafe { cstr_to_opt_owned($name_fn(self.to_native())) };
                f.write_str(name.as_deref().unwrap_or("UNKNOWN"))
            }
        }
    };
}

acl_enum! {
    /// The type of resource an ACL binding applies to.
    AclResourceType,
    RDKafkaResourceType,
    rdsys::rd_kafka_ResourceType_name,
    Unknown {
        /// An unknown resource type.
        Unknown = RD_KAFKA_RESOURCE_UNKNOWN,
        /// Any resource type (filters only).
        Any = RD_KAFKA_RESOURCE_ANY,
        /// A topic.
        Topic = RD_KAFKA_RESOURCE_TOPIC,
        /// A consumer group.
        Group = RD_KAFKA_RESOURCE_GROUP,
        /// The cluster (named `kafka-cluster`).
        Cluster = RD_KAFKA_RESOURCE_BROKER,
        /// A transactional ID.
        TransactionalId = RD_KAFKA_RESOURCE_TRANSACTIONAL_ID,
    }
}

acl_enum! {
    /// How the resource name of an ACL binding is matched.
    AclPatternType,
    rdsys::rd_kafka_ResourcePatternType_t,
    rdsys::rd_kafka_ResourcePatternType_name,
    Unknown {
        /// An unknown pattern type.
        Unknown = RD_KAFKA_RESOURCE_PATTERN_UNKNOWN,
        /// Any pattern type (filters only).
        Any = RD_KAFKA_RESOURCE_PATTERN_ANY,
        /// Every binding that would apply to the name (filters only).
        Match = RD_KAFKA_RESOURCE_PATTERN_MATCH,
        /// The name is the exact resource name.
        Literal = RD_KAFKA_RESOURCE_PATTERN_LITERAL,
        /// The name is a prefix of the resource names.
        Prefixed = RD_KAFKA_RESOURCE_PATTERN_PREFIXED,
    }
}

acl_enum! {
    /// The operation an ACL binding allows or denies.
    AclOperation,
    rdsys::rd_kafka_AclOperation_t,
    rdsys::rd_kafka_AclOperation_name,
    Unknown {
        /// An unknown operation.
        Unknown = RD_KAFKA_ACL_OPERATION_UNKNOWN,
        /// Any operation (filters only).
        Any = RD_KAFKA_ACL_OPERATION_ANY,
        /// All operations.
        All = RD_KAFKA_ACL_OPERATION_ALL,
        /// Read.
        Read = RD_KAFKA_ACL_OPERATION_READ,
        /// Write.
        Write = RD_KAFKA_ACL_OPERATION_WRITE,
        /// Create.
        Create = RD_KAFKA_ACL_OPERATION_CREATE,
        /// Delete.
        Delete = RD_KAFKA_ACL_OPERATION_DELETE,
        /// Alter.
        Alter = RD_KAFKA_ACL_OPERATION_ALTER,
        /// Describe.
        Describe = RD_KAFKA_ACL_OPERATION_DESCRIBE,
        /// Cluster action.
        ClusterAction = RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION,
        /// Describe configs.
        DescribeConfigs = RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS,
        /// Alter configs.
        AlterConfigs = RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS,
        /// Idempotent write.
        IdempotentWrite = RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE,
    }
}

acl_enum! {
    /// Whether an ACL binding allows or denies the operation.
    AclPermissionType,
    rdsys::rd_kafka_AclPermissionType_t,
    rdsys::rd_kafka_AclPermissionType_name,
    Unknown {
        /// An unknown permission type.
        Unknown = RD_KAFKA_ACL_PERMISSION_TYPE_UNKNOWN,
        /// Any permission type (filters only).
        Any = RD_KAFKA_ACL_PERMISSION_TYPE_ANY,
        /// Deny.
        Deny = RD_KAFKA_ACL_PERMISSION_TYPE_DENY,
        /// Allow.
        Allow = RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW,
    }
}

/// An ACL binding: a principal is allowed or denied an operation on the
/// resources that match a pattern.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AclBinding {
    /// The type of the resource.
    pub resource_type: AclResourceType,
    /// The resource name, or prefix.
    pub name: String,
    /// How `name` is matched: [`AclPatternType::Literal`] or
    /// [`AclPatternType::Prefixed`].
    pub pattern_type: AclPatternType,
    /// The principal, for example `User:alice`.
    pub principal: String,
    /// The host, or `*` for every host.
    pub host: String,
    /// The operation.
    pub operation: AclOperation,
    /// Allow or deny.
    pub permission_type: AclPermissionType,
}

/// A filter for ACL bindings: `None` fields and the `Any` values match
/// everything.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AclBindingFilter {
    /// The type of the resource.
    pub resource_type: AclResourceType,
    /// The resource name.
    pub name: Option<String>,
    /// How the name is matched.
    pub pattern_type: AclPatternType,
    /// The principal.
    pub principal: Option<String>,
    /// The host.
    pub host: Option<String>,
    /// The operation.
    pub operation: AclOperation,
    /// The permission type.
    pub permission_type: AclPermissionType,
}

impl AclBindingFilter {
    /// A filter that matches every ACL binding.
    pub fn any() -> AclBindingFilter {
        AclBindingFilter {
            resource_type: AclResourceType::Any,
            name: None,
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission_type: AclPermissionType::Any,
        }
    }
}

impl From<&AclBinding> for AclBindingFilter {
    /// A filter that matches exactly this binding.
    fn from(acl: &AclBinding) -> Self {
        AclBindingFilter {
            resource_type: acl.resource_type,
            name: Some(acl.name.clone()),
            pattern_type: acl.pattern_type,
            principal: Some(acl.principal.clone()),
            host: Some(acl.host.clone()),
            operation: acl.operation,
            permission_type: acl.permission_type,
        }
    }
}

type NativeAclBinding = NativePtr<rdsys::rd_kafka_AclBinding_t>;

unsafe impl KafkaDrop for rdsys::rd_kafka_AclBinding_t {
    const TYPE: &'static str = "ACL binding";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_AclBinding_destroy;
}

fn optional_cstring(value: Option<&str>) -> KafkaResult<Option<CString>> {
    Ok(value.map(CString::new).transpose()?)
}

fn optional_ptr(value: &Option<CString>) -> *const c_char {
    value.as_ref().map_or(std::ptr::null(), |v| v.as_ptr())
}

/// The result of `rd_kafka_AclBinding_new` or `rd_kafka_AclBindingFilter_new`.
fn native_acl_binding(
    ptr: *mut rdsys::rd_kafka_AclBinding_t,
    err_buf: &ErrBuf,
) -> KafkaResult<NativeAclBinding> {
    unsafe { NativeAclBinding::from_ptr(ptr) }.ok_or_else(|| {
        KafkaError::AdminOpCreation(if err_buf.len() == 0 {
            "invalid ACL binding".into()
        } else {
            err_buf.to_string()
        })
    })
}

impl AclBinding {
    fn to_native(&self, err_buf: &mut ErrBuf) -> KafkaResult<NativeAclBinding> {
        let name = CString::new(self.name.as_str())?;
        let principal = CString::new(self.principal.as_str())?;
        let host = CString::new(self.host.as_str())?;
        let ptr = unsafe {
            rdsys::rd_kafka_AclBinding_new(
                self.resource_type.to_native(),
                name.as_ptr(),
                self.pattern_type.to_native(),
                principal.as_ptr(),
                host.as_ptr(),
                self.operation.to_native(),
                self.permission_type.to_native(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };
        native_acl_binding(ptr, err_buf)
    }

    unsafe fn from_native(acl: *const rdsys::rd_kafka_AclBinding_t) -> AclBinding {
        AclBinding {
            resource_type: AclResourceType::from_native(rdsys::rd_kafka_AclBinding_restype(acl)),
            name: cstr_to_opt_owned(rdsys::rd_kafka_AclBinding_name(acl)).unwrap_or_default(),
            pattern_type: AclPatternType::from_native(
                rdsys::rd_kafka_AclBinding_resource_pattern_type(acl),
            ),
            principal: cstr_to_opt_owned(rdsys::rd_kafka_AclBinding_principal(acl))
                .unwrap_or_default(),
            host: cstr_to_opt_owned(rdsys::rd_kafka_AclBinding_host(acl)).unwrap_or_default(),
            operation: AclOperation::from_native(rdsys::rd_kafka_AclBinding_operation(acl)),
            permission_type: AclPermissionType::from_native(
                rdsys::rd_kafka_AclBinding_permission_type(acl),
            ),
        }
    }
}

impl AclBindingFilter {
    fn to_native(&self, err_buf: &mut ErrBuf) -> KafkaResult<NativeAclBinding> {
        let name = optional_cstring(self.name.as_deref())?;
        let principal = optional_cstring(self.principal.as_deref())?;
        let host = optional_cstring(self.host.as_deref())?;
        let ptr = unsafe {
            rdsys::rd_kafka_AclBindingFilter_new(
                self.resource_type.to_native(),
                optional_ptr(&name),
                self.pattern_type.to_native(),
                optional_ptr(&principal),
                optional_ptr(&host),
                self.operation.to_native(),
                self.permission_type.to_native(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };
        native_acl_binding(ptr, err_buf)
    }
}

/// An error code with its message, if the error is set.
unsafe fn error_with_message(
    err: *const rdsys::rd_kafka_error_t,
) -> Option<(RDKafkaErrorCode, String)> {
    if err.is_null() {
        return None;
    }
    let code = rdsys::rd_kafka_error_code(err);
    if code.is_error() {
        let message = cstr_to_opt_owned(rdsys::rd_kafka_error_string(err)).unwrap_or_default();
        Some((code.into(), message))
    } else {
        None
    }
}

/// The result of creating an individual ACL binding.
pub type AclResult = Result<(), (RDKafkaErrorCode, String)>;

/// The result of an individual DeleteAcls filter: the deleted bindings.
pub type DeleteAclsResult = Result<Vec<AclBinding>, (RDKafkaErrorCode, String)>;

unsafe fn build_acl_bindings(
    acls: *mut *const rdsys::rd_kafka_AclBinding_t,
    n: usize,
) -> Vec<AclBinding> {
    (0..n)
        .map(|i| AclBinding::from_native(*acls.add(i)))
        .collect()
}

/// The admin result of an event, or an error if the event is not of the
/// expected type.
fn admin_result<T>(event: &NativeEvent, res: *const T, request: &str) -> KafkaResult<()> {
    if res.is_null() {
        let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
        Err(KafkaError::AdminOpCreation(format!(
            "{} request received response of incorrect type ({})",
            request, typ
        )))
    } else {
        Ok(())
    }
}

struct CreateAclsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for CreateAclsFuture {
    type Output = KafkaResult<Vec<AclResult>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_CreateAcls_result(event.ptr()) };
        admin_result(&event, res, "create ACLs")?;
        let mut n = 0;
        let results = unsafe { rdsys::rd_kafka_CreateAcls_result_acls(res, &mut n) };
        let out = (0..n)
            .map(|i| unsafe {
                match error_with_message(rdsys::rd_kafka_acl_result_error(*results.add(i))) {
                    Some(err) => Err(err),
                    None => Ok(()),
                }
            })
            .collect();
        Poll::Ready(Ok(out))
    }
}

struct DescribeAclsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for DescribeAclsFuture {
    type Output = KafkaResult<Vec<AclBinding>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_DescribeAcls_result(event.ptr()) };
        admin_result(&event, res, "describe ACLs")?;
        let mut n = 0;
        let acls = unsafe { rdsys::rd_kafka_DescribeAcls_result_acls(res, &mut n) };
        Poll::Ready(Ok(unsafe { build_acl_bindings(acls, n) }))
    }
}

struct DeleteAclsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for DeleteAclsFuture {
    type Output = KafkaResult<Vec<DeleteAclsResult>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_DeleteAcls_result(event.ptr()) };
        admin_result(&event, res, "delete ACLs")?;
        let mut n = 0;
        let responses = unsafe { rdsys::rd_kafka_DeleteAcls_result_responses(res, &mut n) };
        let out = (0..n)
            .map(|i| unsafe {
                let response = *responses.add(i);
                if let Some(err) =
                    error_with_message(rdsys::rd_kafka_DeleteAcls_result_response_error(response))
                {
                    return Err(err);
                }
                let mut m = 0;
                let acls =
                    rdsys::rd_kafka_DeleteAcls_result_response_matching_acls(response, &mut m);
                Ok(build_acl_bindings(acls, m))
            })
            .collect();
        Poll::Ready(Ok(out))
    }
}

//
// Create partitions handling
//

/// Configuration for a CreatePartitions operation.
pub struct NewPartitions<'a> {
    /// The name of the topic to which partitions should be added.
    pub topic_name: &'a str,
    /// The total number of partitions after the operation completes.
    pub new_partition_count: usize,
    /// The replica assignments for the new partitions.
    pub assignment: Option<PartitionAssignment<'a>>,
}

impl<'a> NewPartitions<'a> {
    /// Creates a new `NewPartitions`.
    pub fn new(topic_name: &'a str, new_partition_count: usize) -> NewPartitions<'a> {
        NewPartitions {
            topic_name,
            new_partition_count,
            assignment: None,
        }
    }

    /// Sets the partition replica assignment for the new partitions. Only
    /// assignments for newly created replicas should be included.
    pub fn assign(mut self, assignment: PartitionAssignment<'a>) -> NewPartitions<'a> {
        self.assignment = Some(assignment);
        self
    }

    fn to_native(&self, err_buf: &mut ErrBuf) -> KafkaResult<NativeNewPartitions> {
        let name = CString::new(self.topic_name)?;
        if let Some(assignment) = self.assignment {
            // If assignment contains more than self.new_partition_count
            // entries, we'll trip an assertion in librdkafka that crashes the
            // process. Note that this check isn't a guarantee that the
            // partition assignment is valid, since the assignment should only
            // contain entries for the *new* partitions added, and not any
            // existing partitions, but we can let the server handle that
            // validation--we just need to make sure not to crash librdkafka.
            if assignment.len() > self.new_partition_count {
                return Err(KafkaError::AdminOpCreation(format!(
                    "partition assignment for topic '{}' assigns {} partition(s), \
                     which is more than the requested total number of partitions ({})",
                    self.topic_name,
                    assignment.len(),
                    self.new_partition_count,
                )));
            }
        }
        // N.B.: we wrap partition immediately, so that it is destroyed via
        // NativeNewPartitions's Drop implementation if replica assignment or
        // config installation fails.
        let partitions = unsafe {
            NativeNewPartitions::from_ptr(rdsys::rd_kafka_NewPartitions_new(
                name.as_ptr(),
                self.new_partition_count,
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            ))
        }
        .ok_or_else(|| KafkaError::AdminOpCreation(err_buf.to_string()))?;

        if let Some(assignment) = self.assignment {
            for (partition_id, broker_ids) in assignment.iter().enumerate() {
                let res = unsafe {
                    rdsys::rd_kafka_NewPartitions_set_replica_assignment(
                        partitions.ptr(),
                        partition_id as i32,
                        broker_ids.as_ptr() as *mut i32,
                        broker_ids.len(),
                        err_buf.as_mut_ptr(),
                        err_buf.capacity(),
                    )
                };
                check_rdkafka_invalid_arg(res, err_buf)?;
            }
        }
        Ok(partitions)
    }
}

type NativeNewPartitions = NativePtr<RDKafkaNewPartitions>;

unsafe impl KafkaDrop for RDKafkaNewPartitions {
    const TYPE: &'static str = "new partitions";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_NewPartitions_destroy;
}

struct CreatePartitionsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for CreatePartitionsFuture {
    type Output = KafkaResult<Vec<TopicResult>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_CreatePartitions_result(event.ptr()) };
        if res.is_null() {
            let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
            return Poll::Ready(Err(KafkaError::AdminOpCreation(format!(
                "create partitions request received response of incorrect type ({})",
                typ
            ))));
        }
        let mut n = 0;
        let topics = unsafe { rdsys::rd_kafka_CreatePartitions_result_topics(res, &mut n) };
        Poll::Ready(Ok(build_topic_results(topics, n)))
    }
}

//
// Delete records handling
//

type NativeDeleteRecords = NativePtr<RDKafkaDeleteRecords>;

unsafe impl KafkaDrop for RDKafkaDeleteRecords {
    const TYPE: &'static str = "delete records";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_DeleteRecords_destroy;
}

struct DeleteRecordsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for DeleteRecordsFuture {
    type Output = KafkaResult<TopicPartitionList>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_DeleteRecords_result(event.ptr()) };
        if res.is_null() {
            let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
            return Poll::Ready(Err(KafkaError::AdminOpCreation(format!(
                "delete records request received response of incorrect type ({})",
                typ
            ))));
        }
        let tpl = unsafe {
            let tpl = rdsys::rd_kafka_DeleteRecords_result_offsets(res);
            TopicPartitionList::from_ptr(rdsys::rd_kafka_topic_partition_list_copy(tpl))
        };
        Poll::Ready(Ok(tpl))
    }
}

//
// Describe configs handling
//

/// The result of an individual DescribeConfig operation.
pub type ConfigResourceResult = Result<ConfigResource, RDKafkaErrorCode>;

/// Specification of a configurable resource.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ResourceSpecifier<'a> {
    /// A topic resource, identified by its name.
    Topic(&'a str),
    /// A group resource, identified by its ID.
    Group(&'a str),
    /// A broker resource, identified by its ID.
    Broker(i32),
}

/// A `ResourceSpecifier` that owns its data.
#[derive(Debug, Eq, PartialEq)]
pub enum OwnedResourceSpecifier {
    /// A topic resource, identified by its name.
    Topic(String),
    /// A group resource, identified by its ID.
    Group(String),
    /// A broker resource, identified by its ID.
    Broker(i32),
}

/// The source of a configuration entry.
#[derive(Debug, Eq, PartialEq)]
pub enum ConfigSource {
    /// Unknown. Note that Kafka brokers before v1.1.0 do not reliably provide
    /// configuration source information.
    Unknown,
    /// A dynamic topic configuration.
    DynamicTopic,
    /// A dynamic broker configuration.
    DynamicBroker,
    /// The default dynamic broker configuration.
    DynamicDefaultBroker,
    /// The static broker configuration.
    StaticBroker,
    /// The hardcoded default configuration.
    Default,
}

/// An individual configuration parameter for a `ConfigResource`.
#[derive(Debug, Eq, PartialEq)]
pub struct ConfigEntry {
    /// The name of the configuration parameter.
    pub name: String,
    /// The value of the configuration parameter.
    pub value: Option<String>,
    /// The source of the configuration parameter.
    pub source: ConfigSource,
    /// Whether the configuration parameter is read only.
    pub is_read_only: bool,
    /// Whether the configuration parameter currently has the default value.
    pub is_default: bool,
    /// Whether the configuration parameter contains sensitive data.
    pub is_sensitive: bool,
}

/// A configurable resource and its current configuration values.
#[derive(Debug)]
pub struct ConfigResource {
    /// Identifies the resource.
    pub specifier: OwnedResourceSpecifier,
    /// The current configuration parameters.
    pub entries: Vec<ConfigEntry>,
}

impl ConfigResource {
    /// Builds a `HashMap` of configuration entries, keyed by configuration
    /// entry name.
    pub fn entry_map(&self) -> HashMap<&str, &ConfigEntry> {
        self.entries.iter().map(|e| (&*e.name, e)).collect()
    }

    /// Searches the configuration entries to find the named parameter.
    ///
    /// For more efficient lookups, use `entry_map` to build a `HashMap`
    /// instead.
    pub fn get(&self, name: &str) -> Option<&ConfigEntry> {
        self.entries.iter().find(|e| e.name == name)
    }
}

type NativeConfigResource = NativePtr<RDKafkaConfigResource>;

unsafe impl KafkaDrop for RDKafkaConfigResource {
    const TYPE: &'static str = "config resource";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_ConfigResource_destroy;
}

fn extract_config_specifier(
    resource: *const RDKafkaConfigResource,
) -> KafkaResult<OwnedResourceSpecifier> {
    let typ = unsafe { rdsys::rd_kafka_ConfigResource_type(resource) };
    match typ {
        RDKafkaResourceType::RD_KAFKA_RESOURCE_TOPIC => {
            let name = unsafe { cstr_to_owned(rdsys::rd_kafka_ConfigResource_name(resource)) };
            Ok(OwnedResourceSpecifier::Topic(name))
        }
        RDKafkaResourceType::RD_KAFKA_RESOURCE_GROUP => {
            let name = unsafe { cstr_to_owned(rdsys::rd_kafka_ConfigResource_name(resource)) };
            Ok(OwnedResourceSpecifier::Group(name))
        }
        RDKafkaResourceType::RD_KAFKA_RESOURCE_BROKER => {
            let name = unsafe { CStr::from_ptr(rdsys::rd_kafka_ConfigResource_name(resource)) }
                .to_string_lossy();
            match name.parse::<i32>() {
                Ok(id) => Ok(OwnedResourceSpecifier::Broker(id)),
                Err(_) => Err(KafkaError::AdminOpCreation(format!(
                    "bogus broker ID in kafka response: {}",
                    name
                ))),
            }
        }
        _ => Err(KafkaError::AdminOpCreation(format!(
            "bogus resource type in kafka response: {:?}",
            typ
        ))),
    }
}

fn extract_config_source(config_source: RDKafkaConfigSource) -> KafkaResult<ConfigSource> {
    match config_source {
        RDKafkaConfigSource::RD_KAFKA_CONFIG_SOURCE_UNKNOWN_CONFIG => Ok(ConfigSource::Unknown),
        RDKafkaConfigSource::RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG => {
            Ok(ConfigSource::DynamicTopic)
        }
        RDKafkaConfigSource::RD_KAFKA_CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG => {
            Ok(ConfigSource::DynamicBroker)
        }
        RDKafkaConfigSource::RD_KAFKA_CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG => {
            Ok(ConfigSource::DynamicDefaultBroker)
        }
        RDKafkaConfigSource::RD_KAFKA_CONFIG_SOURCE_STATIC_BROKER_CONFIG => {
            Ok(ConfigSource::StaticBroker)
        }
        RDKafkaConfigSource::RD_KAFKA_CONFIG_SOURCE_DEFAULT_CONFIG => Ok(ConfigSource::Default),
        _ => Err(KafkaError::AdminOpCreation(format!(
            "bogus config source type in kafka response: {:?}",
            config_source,
        ))),
    }
}

struct DescribeConfigsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for DescribeConfigsFuture {
    type Output = KafkaResult<Vec<ConfigResourceResult>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_DescribeConfigs_result(event.ptr()) };
        if res.is_null() {
            let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
            return Poll::Ready(Err(KafkaError::AdminOpCreation(format!(
                "describe configs request received response of incorrect type ({})",
                typ
            ))));
        }
        let mut n = 0;
        let resources = unsafe { rdsys::rd_kafka_DescribeConfigs_result_resources(res, &mut n) };
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let resource = unsafe { *resources.add(i) };
            let specifier = extract_config_specifier(resource)?;
            let mut entries_out = Vec::new();
            let mut n = 0;
            let entries = unsafe { rdsys::rd_kafka_ConfigResource_configs(resource, &mut n) };
            for j in 0..n {
                let entry = unsafe { *entries.add(j) };
                let name = unsafe { cstr_to_owned(rdsys::rd_kafka_ConfigEntry_name(entry)) };
                let value = unsafe {
                    let value = rdsys::rd_kafka_ConfigEntry_value(entry);
                    if value.is_null() {
                        None
                    } else {
                        Some(cstr_to_owned(value))
                    }
                };
                entries_out.push(ConfigEntry {
                    name,
                    value,
                    source: extract_config_source(unsafe {
                        rdsys::rd_kafka_ConfigEntry_source(entry)
                    })?,
                    is_read_only: unsafe { rdsys::rd_kafka_ConfigEntry_is_read_only(entry) } != 0,
                    is_default: unsafe { rdsys::rd_kafka_ConfigEntry_is_default(entry) } != 0,
                    is_sensitive: unsafe { rdsys::rd_kafka_ConfigEntry_is_sensitive(entry) } != 0,
                });
            }
            out.push(Ok(ConfigResource {
                specifier,
                entries: entries_out,
            }))
        }
        Poll::Ready(Ok(out))
    }
}

//
// Alter configs handling
//

/// The result of an individual AlterConfig operation.
pub type AlterConfigsResult =
    Result<OwnedResourceSpecifier, (OwnedResourceSpecifier, RDKafkaErrorCode)>;

/// Configuration for an AlterConfig operation.
pub struct AlterConfig<'a> {
    /// Identifies the resource to be altered.
    pub specifier: ResourceSpecifier<'a>,
    /// The configuration parameters to be updated.
    pub entries: HashMap<&'a str, &'a str>,
}

impl<'a> AlterConfig<'a> {
    /// Creates a new `AlterConfig`.
    pub fn new(specifier: ResourceSpecifier<'_>) -> AlterConfig<'_> {
        AlterConfig {
            specifier,
            entries: HashMap::new(),
        }
    }

    /// Sets the configuration parameter named `key` to the specified `value`.
    pub fn set(mut self, key: &'a str, value: &'a str) -> AlterConfig<'a> {
        self.entries.insert(key, value);
        self
    }

    fn to_native(&self, err_buf: &mut ErrBuf) -> KafkaResult<NativeConfigResource> {
        let (name, typ) = match self.specifier {
            ResourceSpecifier::Topic(name) => (
                CString::new(name)?,
                RDKafkaResourceType::RD_KAFKA_RESOURCE_TOPIC,
            ),
            ResourceSpecifier::Group(name) => (
                CString::new(name)?,
                RDKafkaResourceType::RD_KAFKA_RESOURCE_GROUP,
            ),
            ResourceSpecifier::Broker(id) => (
                CString::new(format!("{}", id))?,
                RDKafkaResourceType::RD_KAFKA_RESOURCE_BROKER,
            ),
        };
        // N.B.: we wrap config immediately, so that it is destroyed via the
        // NativeNewTopic's Drop implementation if config installation fails.
        let config = unsafe {
            NativeConfigResource::from_ptr(rdsys::rd_kafka_ConfigResource_new(typ, name.as_ptr()))
                .unwrap()
        };
        for (key, val) in &self.entries {
            let key_c = CString::new(*key)?;
            let val_c = CString::new(*val)?;
            let res = unsafe {
                rdsys::rd_kafka_ConfigResource_set_config(
                    config.ptr(),
                    key_c.as_ptr(),
                    val_c.as_ptr(),
                )
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }
        Ok(config)
    }
}

struct AlterConfigsFuture {
    rx: oneshot::Receiver<NativeEvent>,
}

impl Future for AlterConfigsFuture {
    type Output = KafkaResult<Vec<AlterConfigsResult>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let event = ready!(self.rx.poll_unpin(cx)).map_err(|_| KafkaError::Canceled)?;
        event.check_error()?;
        let res = unsafe { rdsys::rd_kafka_event_AlterConfigs_result(event.ptr()) };
        if res.is_null() {
            let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
            return Poll::Ready(Err(KafkaError::AdminOpCreation(format!(
                "alter configs request received response of incorrect type ({})",
                typ
            ))));
        }
        let mut n = 0;
        let resources = unsafe { rdsys::rd_kafka_AlterConfigs_result_resources(res, &mut n) };
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let resource = unsafe { *resources.add(i) };
            let specifier = extract_config_specifier(resource)?;
            out.push(Ok(specifier));
        }
        Poll::Ready(Ok(out))
    }
}
