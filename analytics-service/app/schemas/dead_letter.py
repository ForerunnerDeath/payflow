from datetime import UTC, datetime

from pydantic import BaseModel, Field


class DeadLetterMessage(BaseModel):
    source_topic: str
    source_partition: int
    source_offset: int
    source_key_base64: str | None
    source_value_base64: str
    error_type: str
    error_message: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
