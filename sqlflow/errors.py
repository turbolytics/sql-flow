import enum


class Policy(str, enum.Enum):
    RAISE = "raise"
    IGNORE = "ignore"
    DLQ = "dlq"