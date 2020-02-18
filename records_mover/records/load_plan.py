from .processing_instructions import ProcessingInstructions
from .records_format import BaseRecordsFormat


class RecordsLoadPlan:
    def __init__(self,
                 processing_instructions: ProcessingInstructions,
                 records_format: BaseRecordsFormat) -> None:
        self.records_format = records_format
        self.processing_instructions = processing_instructions
