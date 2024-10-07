from datetime import datetime
from enum import Enum
from typing import List

from pydantic import BaseModel, Field


class JobType(str, Enum):
    data_retention = "data_retention"
    message_export = "message_export"
    elasticsearch_post_indexing = "elasticsearch_post_indexing"
    elasticsearch_post_aggregation = "elasticsearch_post_aggregation"
    bleve_post_indexing = "bleve_post_indexing"
    ldap_sync = "ldap_sync"
    migrations = "migrations"
    plugins = "plugins"
    expiry_notify = "expiry_notify"
    product_notices = "product_notices"
    active_users = "active_users"
    import_process = "import_process"
    import_delete = "import_delete"
    export_process = "export_process"
    export_delete = "export_delete"
    cloud = "cloud"
    resend_invitation_email = "resend_invitation_email"
    extract_content = "extract_content"
    last_accessible_post = "last_accessible_post"
    last_accessible_file = "last_accessible_file"
    upgrade_notify_admin = "upgrade_notify_admin"
    trial_notify_admin = "trial_notify_admin"
    post_persistent_notifications = "post_persistent_notifications"
    install_plugin_notify_admin = "install_plugin_notify_admin"
    hosted_purchase_screening = "hosted_purchase_screening"
    s3_path_migration = "s3_path_migration"
    cleanup_desktop_tokens = "cleanup_desktop_tokens"
    delete_empty_drafts_migration = "delete_empty_drafts_migration"
    refresh_post_stats = "refresh_post_stats"
    delete_orphan_drafts_migration = "delete_orphan_drafts_migration"
    export_users_to_csv = "export_users_to_csv"
    delete_dms_preferences_migration = "delete_dms_preferences_migration"


class JobStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    success = "success"
    error = "error"
    cancel_requested = "cancel_requested"
    canceled = "canceled"
    warning = "warning"


class JobV1(BaseModel):
    id: str
    type: JobType
    priority: int
    createat: datetime
    startat: datetime
    lastactivityat: datetime
    status: JobStatus
    progress: int
    data: dict


class SupportPacketV1(BaseModel):
    # Based on https://github.com/mattermost/mattermost/blob/master/server/public/model/support_packet.go#L15
    server_os: str = Field(description="The operating system of the server")
    server_architecture: str = Field(description="The architecture of the server, i.e. amd64")
    server_version: str = Field(description="The version of the Mattermost server")
    build_hash: str = Field(description="The build hash of the Mattermost server")

    # DB
    database_type: str = Field(description="The type of database being used, i.e. postgres or mysql")
    database_version: str = Field(description="The version of the database being used")
    database_schema_version: str = Field(description="The schema version of the database being used")
    websocket_connections: int = Field(description="The number of websocket connections")
    master_db_connections: int = Field(description="The number of master database connections")
    read_db_connections: int = Field(description="The number of read database connections")

    # Cluster
    cluster_id: str | None = Field(None, description="The cluster ID, if server is running in cluster mode")

    # File store
    file_driver: str | None = Field(None, description="The file store driver being used, i.e. local or s3")
    file_status: str | None = Field(None, description="The status of the file store")

    # LDAP
    ldap_vendor_name: str | None = Field(None)
    ldap_verndor_version: str | None = Field(None)

    # ElasticSearch
    elastic_server_version: str | None = Field(None)
    elastic_server_plugins: str | None = Field(None)

    # License
    license_to: str = Field(description="The name of the license owner, extracted from the license.")
    license_supported_users: int = Field(description="The number of supported users in the license.")
    license_is_trial: bool | None = Field(None, description="Whether the license is a trial license.")

    # Server Stats
    active_users: int = Field(description="The number of unique active users")
    daily_active_users: int = Field(description="The number of daily active users")
    monthly_active_users: int = Field(description="The number of monthly active users")
    inactive_user_count: int = Field(description="The number of inactive users")
    total_posts: int = Field(description="The total number of posts")
    total_channels: int = Field(description="The total number of channels")
    total_teams: int = Field(description="The total number of teams")

    # Jobs
    data_retention_jobs: List[JobV1] | None = Field(None, description="Data retention jobs")
    bleve_post_indexing_jobs: List[JobV1] | None = Field(None, description="Bleve post indexing jobs")
    message_export_jobs: List[JobV1] | None = Field(None, description="Message export jobs")
    elastic_post_indexing_jobs: List[JobV1] | None = Field(None, description="Bleve post indexing jobs")
    elastic_post_aggregation_jobs: List[JobV1] | None = Field(None, description="Elasticsearch post aggregation jobs")
    ldap_sync_jobs: List[JobV1] | None = Field(None, description="LDAP sync jobs")
    migration_jobs: List[JobV1] | None = Field(None, description="Migration jobs")
    compliance_jobs: List[JobV1] | None = Field(None, description="Compliance jobs")

    def all_jobs(self):
        all_jobs = [
            self.data_retention_jobs,
            self.bleve_post_indexing_jobs,
            self.message_export_jobs,
            self.elastic_post_indexing_jobs,
            self.elastic_post_aggregation_jobs,
            self.ldap_sync_jobs,
            self.migration_jobs,
            self.compliance_jobs,
        ]
        return [job for job_list in all_jobs if job_list for job in job_list]
