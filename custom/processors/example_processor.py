"""Example custom processor.

A processor is a pipeline step. Subclass pangolin's BaseProcessor to get
registry pattern matching, DataFacility IO and logging for free; implement
`execute()` and write one report per file (traceability convention).

This AuditProcessor reports row/column counts (and null counts when enabled
in config/registries/2_audit.yaml) and forwards the data unchanged.
"""

from pangolin.config.run_context import RunContext
from pangolin.engine.common.exceptions import AllFilesFailedError, NoInputFilesError
from pangolin.engine.processors.BaseProcessor import BaseProcessor
from pangolin.engine.reporter import Reporter


class AuditProcessor(BaseProcessor):
    def __init__(self, CTX: RunContext, name: str, report_folder: str,
                 input_folder: str, output_folder: str = None):
        super().__init__(CTX, name, input_folder, output_folder)
        self.reporter = Reporter(CTX, report_folder, step_name=name)

    def execute(self, file_paths=None):
        results = {}

        for full_path, data, pattern, rel_path, errors in self.process_files(file_paths):
            if errors:
                self.reporter.write_report(rel_path, errors)
                results[full_path] = {"success": False, "errors": errors}
                continue

            self.log.info(f"Auditing {rel_path}")
            messages = [f"rows={len(data)}", f"columns={len(data.columns)}"]

            if self.registry[pattern].get("count_nulls", False):
                null_counts = data.null_count()
                for column in data.columns:
                    nulls = int(null_counts[column][0])
                    if nulls:
                        messages.append(f"null values in '{column}': {nulls}")

            self.write_file(data, rel_path)
            self.reporter.write_report(rel_path, messages)
            results[full_path] = {"success": True}

        if not results:
            raise NoInputFilesError(self.name, str(self.input_node.path))
        if not any(r["success"] for r in results.values()):
            raise AllFilesFailedError(f"[{self.name}] All files failed the audit step.")

        return results
