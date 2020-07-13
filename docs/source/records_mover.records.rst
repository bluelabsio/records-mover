records\_mover.records package
==============================

Subpackages
-----------

.. toctree::
   :maxdepth: 4

   records_mover.records.sources
   records_mover.records.targets

Submodules
----------

records\_mover.records.base\_records\_format module
---------------------------------------------------

.. autoclass:: records_mover.records.base_records_format.BaseRecordsFormat
   :members:
   :show-inheritance:
   :special-members: __init__

Module contents
---------------

.. comment:

   I exclude PartialRecordsHints entirely below because Sphinx doesn't
   yet support TypedDict types that have hyphens as part of the keys.
   Example error on 'make html':

   WARNING: invalid signature for autoinstanceattribute
   ('records_mover.records::PartialRecordsHints.header-row')

   I exclude RecordsSchema as we haven't defined yet exactly what
   public API to export.

   https://github.com/bluelabsio/records-mover/issues/97

   I exclude RecordsFormat as it's generally an internal factory
   method useful when reading undifferentiated JSON files - a general
   user ought to be able to select their records format in advance and
   use the correct subclass.

   I exclude UntypedRecordsHints and RecordsFormatType as it hasn't
   been used in any other part of the public interface.

   I exclude DelimitedVariant as there's no way to specify a
   docstring, so I add out-of-band documentation below.

.. automodule:: records_mover.records
   :members:
   :undoc-members:
   :show-inheritance:
   :exclude-members: DelimitedRecordsFormat, ParquetRecordsFormat, ProcessingInstructions, PartialRecordsHints, RecordsSchema, RecordsFormat, UntypedRecordsHints, RecordsFormatType, DelimitedVariant

   .. autoclass:: records_mover.records.DelimitedRecordsFormat
      :undoc-members:
      :show-inheritance:
      :special-members: __init__

   .. autoclass:: records_mover.records.ParquetRecordsFormat
      :undoc-members:
      :show-inheritance:
      :special-members: __init__

   .. autoclass:: records_mover.records.ProcessingInstructions
      :undoc-members:
      :show-inheritance:
      :special-members: __init__

   .. py:data:: DelimitedVariant
      :module: records_mover.records

      Valid string values for the variant of a delimited records format.
      Variants specify a default set of parsing hints for how the delimited
      file is formatted.  See the `records format specification
      <https://github.com/bluelabsio/records-mover/blob/master/docs/RECORDS_SPEC.md>`_
      for semantics of each.

      alias of Literal[dumb, csv, bigquery, bluelabs, vertica]
