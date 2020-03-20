from .processing_instructions import ProcessingInstructions
from .records_format import BaseRecordsFormat, DelimitedRecordsFormat


class RecordsUnloadPlan:
    def __init__(self,
                 records_format: BaseRecordsFormat = DelimitedRecordsFormat(),
                 processing_instructions: ProcessingInstructions = ProcessingInstructions()) -> \
                 None:
        self.records_format = records_format
        self.processing_instructions = processing_instructions
