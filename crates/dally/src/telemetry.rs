//! Trace export over OTLP, built with the `otel` feature.
//!
//! Export only starts when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the traces-only
//! `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) is set. Everything else is
//! configured with the standard `OTEL_*` variables, such as
//! `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES`. The protocol is OTLP
//! over HTTP with protobuf, which every collector accepts on port 4318.

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

/// Flushes and stops the exporter when dropped. Drop it outside the async
/// runtime: the exporter uses a blocking HTTP client.
pub struct Guard(Option<SdkTracerProvider>);

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(provider) = self.0.take()
            && let Err(err) = provider.shutdown()
        {
            eprintln!("dally: flushing OpenTelemetry traces failed: {err}");
        }
    }
}

/// The tracing layer that exports spans, if export is configured.
pub fn layer<S>() -> (Option<impl Layer<S>>, Guard)
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    let configured = [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ]
    .iter()
    .any(|var| std::env::var_os(var).is_some());
    if !configured {
        return (None, Guard(None));
    }

    let exporter = match SpanExporter::builder().with_http().build() {
        Ok(exporter) => exporter,
        Err(err) => {
            // Logging is not set up yet, and a broken exporter should not
            // stop the queue from running.
            eprintln!("dally: OpenTelemetry export is disabled: {err}");
            return (None, Guard(None));
        }
    };

    let mut resource = Resource::builder();
    if std::env::var_os("OTEL_SERVICE_NAME").is_none() {
        resource = resource.with_service_name("dally");
    }
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.build())
        .build();
    let tracer = provider.tracer("dally");

    let layer = tracing_opentelemetry::layer().with_tracer(tracer);
    (Some(layer), Guard(Some(provider)))
}
