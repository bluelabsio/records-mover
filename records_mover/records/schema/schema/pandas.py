from pandas import DataFrame
from .known_representation import RecordsSchemaKnownRepresentation
from typing import Dict, TYPE_CHECKING
from ...processing_instructions import ProcessingInstructions
if TYPE_CHECKING:
    from ..field import RecordsSchemaField  # noqa
    from ..schema import RecordsSchema  # noqa


def schema_from_dataframe(df: DataFrame,
                          processing_instructions: ProcessingInstructions,
                          include_index: bool) -> 'RecordsSchema':
    from records_mover.records.schema import RecordsSchema  # noqa
    from records_mover.records.schema.field import RecordsSchemaField  # noqa
    fields = []
    origin_representation = \
        RecordsSchemaKnownRepresentation.from_dataframe(df, processing_instructions)
    known_representations: Dict[str, RecordsSchemaKnownRepresentation] = {
        'origin': origin_representation
    }

    if include_index:
        fields.append(RecordsSchemaField.
                      from_index(df.index,
                                 processing_instructions=processing_instructions))
    for column in df:
        fields.append(RecordsSchemaField.
                      from_series(df[column],
                                  processing_instructions=processing_instructions))

    return RecordsSchema(fields=fields,
                         known_representations=known_representations)


def refine_schema_from_dataframe(records_schema: 'RecordsSchema',
                                 df: DataFrame,
                                 processing_instructions:
                                 ProcessingInstructions = ProcessingInstructions()) ->\
        'RecordsSchema':
    from records_mover.records.schema import RecordsSchema

    max_sample_size = processing_instructions.max_inference_rows
    total_rows = len(df.index)
    if max_sample_size is not None and max_sample_size < total_rows:
        sampled_df = df.sample(n=max_sample_size)
    else:
        sampled_df = df
    rows_sampled = len(sampled_df.index)

    fields = [
        field.refine_from_series(sampled_df[field.name],
                                 total_rows=total_rows,
                                 rows_sampled=rows_sampled)
        for field in records_schema.fields
    ]
    return RecordsSchema(fields=fields,
                         known_representations=records_schema.known_representations)
