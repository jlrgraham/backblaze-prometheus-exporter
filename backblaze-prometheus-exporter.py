#!/usr/bin/env python3

import json
import os
import prometheus_client as prom
import time

from b2sdk.v2 import B2Api, InMemoryAccountInfo


prom.REGISTRY.unregister(prom.PROCESS_COLLECTOR)
prom.REGISTRY.unregister(prom.PLATFORM_COLLECTOR)
prom.REGISTRY.unregister(prom.GC_COLLECTOR)

last_update = prom.Gauge("backblaze_b2_last_update_time",
                         "last update timestamp for a given bucket, in unix time.", ['bucket'])
total_size = prom.Gauge("backblaze_b2_total_size",
                        "total size of contents for a given bucket, in bytes.", ['bucket'])
object_count = prom.Gauge("backblaze_b2_object_count",
                        "total count of contents for a given bucket, in objects.", ['bucket'])


def init_b2(application_key_id, application_key):
    info = InMemoryAccountInfo()

    b2_api = B2Api(info)
    b2_api.authorize_account("production", application_key_id, application_key)

    return b2_api


def get_bucket_names(b2_api):
    for bucket in b2_api.list_buckets():
        yield bucket.name


def get_bucket_stats(b2_api, bucket_name):
    bucket = b2_api.get_bucket_by_name(bucket_name)

    total_size = 0
    timestamps = []
    object_count = 0

    for file_version, folder_name in bucket.ls(latest_only=False, recursive=True):
        total_size += file_version.size
        timestamps.append(file_version.upload_timestamp)
        object_count += 1

    latest_timestamp = max(timestamps)

    stats = {
        "total_size": total_size,
        "latest_timestamp": latest_timestamp,
        "object_count": object_count,
    }

    return stats


def update_gauges(b2_api):
    bucket_data = {
        bucket_name: get_bucket_stats(b2_api, bucket_name)
        for bucket_name in get_bucket_names(b2_api)
    }

    last_update.clear()
    total_size.clear()
    object_count.clear()

    for bucket_name, stats in bucket_data.items():
        last_update.labels(bucket=bucket_name).set(stats['latest_timestamp'])
        total_size.labels(bucket=bucket_name).set(stats['total_size'])
        object_count.labels(bucket=bucket_name).set(stats['object_count'])


def main():
    application_key_id = os.environ.get("B2_APPLICATION_KEY_ID", default=None)
    application_key = os.environ.get("B2_APPLICATION_KEY", default=None)

    metrics_port = int(os.environ.get("METRICS_PORT", default="9139"))
    update_interval = int(os.environ.get("UPDATE_INTERVAL", default=str(60*60*12)))

    if application_key_id is None:
        print("Error: B2_APPLICATION_KEY_ID must be set")
        return 1
    if application_key is None:
        print("Error: B2_APPLICATION_KEY must be set")
        return 1

    b2_api = init_b2(
        application_key_id=application_key_id,
        application_key=application_key
    )

    print(f"Starting metrics server on port {metrics_port}")
    prom.start_http_server(metrics_port)

    while True:
        update_gauges(b2_api=b2_api)
        print("updating.")
        time.sleep(update_interval)


if __name__ == "__main__":
    main()
