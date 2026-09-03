"""
Example pipeline: backup -> raw validation -> transform -> audit -> delivery.

Demonstrates everything a pangolin project is made of:
- default processors shipped with pangolin (BackupRestore, Validator,
  DataTransformer, FileDispatcher)
- a custom processor (custom/processors/example_processor.py)
- custom validators/transformers registered via decorators (custom/)
- registry-driven pattern matching (config/registries/*.yaml)

Try it: drop a CSV matching '*sales*' into data/input/ (a sample is created
by `pangolin init`) and run `pangolin run example_pipeline`.
"""

from prefect import flow, get_run_logger

# Importing these modules registers the project's custom validators and
# transformers into the registries used by pangolin's processors.
import custom.transformers  # noqa: F401
import custom.validators  # noqa: F401

from custom.processors.example_processor import AuditProcessor
from pangolin.config.run_context import RunContext
from pangolin.config.settings import get_settings
from pangolin.engine.processors.BackupRestore import BackupRestore
from pangolin.engine.processors.DataTransformer import DataTransformer
from pangolin.engine.processors.DataValidator import Validator
from pangolin.engine.processors.FileDispatcher import FileDispatcher


@flow(name="Backup Input")
def backup_flow(CTX: RunContext):
    """Backup current input files to backup/<run_id>/."""
    S = get_settings()
    backup = BackupRestore(CTX, name="backup", input_folder=S.INPUT_FOLDER_NAME, output_folder="backup")
    backup.execute()


@flow(name="0 - Raw Validation")
def raw_validation_flow(CTX: RunContext):
    """Validate raw input files against config/registries/0_raw_validation.yaml."""
    S = get_settings()
    validator = Validator(
        CTX,
        name="0_validator",
        report_folder=S.REPORTS_FOLDER_NAME,
        input_folder=S.INPUT_FOLDER_NAME,
        output_folder="staging.0_validator",
    )
    validator.execute()


@flow(name="1 - Transform")
def transform_flow(CTX: RunContext):
    """Apply the transforms declared in config/registries/1_transform.yaml."""
    S = get_settings()
    transformer = DataTransformer(
        CTX,
        name="1_transform",
        report_folder=S.REPORTS_FOLDER_NAME,
        input_folder="staging.0_validator",
        output_folder="staging.1_transform",
    )
    transformer.execute()


@flow(name="2 - Audit (custom processor)")
def audit_flow(CTX: RunContext):
    """Run the example custom processor (see custom/processors/)."""
    S = get_settings()
    auditor = AuditProcessor(
        CTX,
        name="2_audit",
        report_folder=S.REPORTS_FOLDER_NAME,
        input_folder="staging.1_transform",
        output_folder="staging.2_audit",
    )
    auditor.execute()


@flow(name="3 - Delivery Dispatch")
def delivery_flow(CTX: RunContext):
    """Dispatch processed files to delivery/ per config/registries/3_dispatcher.yaml."""
    S = get_settings()
    dispatcher = FileDispatcher(
        CTX,
        name="3_dispatcher",
        report_folder=S.REPORTS_FOLDER_NAME,
        input_folder="staging.2_audit",
        output_folder=S.DELIVERY_FOLDER_NAME,
        rm_from_input_folder=True,
    )
    dispatcher.execute()


@flow(name="Example Pipeline", description="Validate, transform, audit and deliver sales files")
def example_pipeline():
    logger = get_run_logger()
    CTX = RunContext()
    logger.info(f"Example pipeline started - PANGOLIN_RUN_ID: {CTX.RUN_ID}")

    s_init = backup_flow(CTX, return_state=True)
    s0 = raw_validation_flow(CTX, return_state=True, wait_for=[s_init])
    s1 = transform_flow(CTX, return_state=True, wait_for=[s0])
    s2 = audit_flow(CTX, return_state=True, wait_for=[s1])
    delivery_flow(CTX, return_state=True, wait_for=[s2])

    logger.info("Example pipeline ended successfully")


# Marks the flow to expose for this module (required by the pipeline registry).
PIPELINE = example_pipeline
