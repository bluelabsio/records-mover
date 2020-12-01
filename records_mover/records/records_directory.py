import json
import logging
from .records_format_file import RecordsFormatFile
from .records_schema_sql_file import RecordsSchemaSqlFile
from .records_schema_json_file import RecordsSchemaJsonFile
from .schema import RecordsSchema
from urllib.parse import urlparse
from ..url.base import BaseDirectoryUrl, BaseFileUrl
from .records_format import BaseRecordsFormat, DelimitedRecordsFormat
from typing import Mapping, IO, List, Optional
from .records_types import UrlDetails, RecordsManifestWithLength, LegacyRecordsManifest


logger = logging.getLogger(__name__)


class RecordsDirectory:
    def __init__(self,
                 records_loc: BaseDirectoryUrl) -> None:
        self.loc = records_loc
        self.scheme = records_loc.scheme
        self.schema_sql_file = RecordsSchemaSqlFile(self.loc)
        self.schema_json_file = RecordsSchemaJsonFile(self.loc)
        self.format_file = RecordsFormatFile(self.loc)

    def save_schema(self, records_schema: RecordsSchema) -> None:
        json = records_schema.to_json()
        self.schema_json_file.save_schema_json(json)

    def load_schema_sql_from_sql_file(self) -> Optional[str]:
        return self.schema_sql_file.load_schema_sql()

    def load_schema_json(self) -> Optional[str]:
        return self.schema_json_file.load_schema_json()

    def load_schema_json_obj(self) -> Optional[RecordsSchema]:
        json = self.load_schema_json()
        if json is None:
            return None
        else:
            return RecordsSchema.from_json(json)

    def save_format(self, records_format: BaseRecordsFormat) -> None:
        self.format_file.save_format(records_format)

    def load_format(self, fail_if_dont_understand: bool) -> BaseRecordsFormat:
        return self.format_file.load_format(fail_if_dont_understand)

    def save_fileobjs(self,
                      fileobjs_by_target_names: Mapping[str, IO[bytes]],
                      records_schema: Optional[RecordsSchema]=None,
                      records_format: Optional[BaseRecordsFormat]=None) \
            -> UrlDetails:
        """Write out a full records directory from file objects."""
        url_details: UrlDetails = self.save_data_from_fileobjs(fileobjs_by_target_names)
        self.save_preliminary_manifest(url_details)
        if records_schema:
            self.save_schema(records_schema)
        if records_format:
            self.save_format(records_format)
        self.finalize_manifest()
        # Help ensure that future use of this directory will succeed
        self.loc.file_in_this_directory('_manifest').wait_to_exist()
        return url_details

    def save_data_from_fileobjs(self,
                                fileobjs_by_target_names: Mapping[str,
                                                                  IO[bytes]]) -> UrlDetails:
        """
        Write out just the datafiles into the records directory.

        Prefer save_fileobjs when writing a complete records directory.
        """
        url_details: UrlDetails = {}
        for target_name, fileobj in fileobjs_by_target_names.items():
            target_loc = self.loc.file_in_this_directory(target_name)
            logger.info(f"Uploading {target_loc.url}")
            length: int = target_loc.upload_fileobj(fileobj)
            url_details[target_loc.url] = {
                'content_length': length,
            }
        return url_details

    def _build_manifest(self,
                        url_details: UrlDetails) -> RecordsManifestWithLength:
        return {
            "entries": [
                {
                    "url": url,
                    "mandatory": True,
                    "meta": details
                }
                for url, details in url_details.items()
            ]
        }

    def save_preliminary_manifest(self, url_details: Optional[UrlDetails]=None) -> None:
        """Save manifest file to records directory.

         :param urls_for_manifest: list of URLs that should be
          listed in manifest. Defaults to None, in which case it lists
          files in directory (which is generally not advised because
          it could capture unintentional files).
        """

        manifest_loc = self.loc.file_in_this_directory('manifest')
        if url_details is None:
            logger.warning("Building manifest by listing directory contents")
            url_details = {
                loc.url: {
                    'content_length': loc.size()
                }
                for loc in self.loc.files_in_directory()
                if not loc.filename().startswith('_') and loc.filename() != 'manifest'
            }
        logger.info(f"Storing manifest into {manifest_loc.url}")
        manifest_loc.store_string(json.dumps(self._build_manifest(url_details)))

    def _filename_of_url(self, url: str) -> str:
        path = urlparse(url).path
        return path.split('/')[-1]

    def copy_to(self, new_loc: BaseDirectoryUrl) -> 'RecordsDirectory':
        logger.info(f"Copying files from {self.loc} to {new_loc}...")
        new_loc = self.loc.copy_to(new_loc)
        # rebuild manifest to point to new URLs.
        new_directory = RecordsDirectory(records_loc=new_loc)
        # regenerate manifest with new URLs
        old_urls: List[str] = self.manifest_entry_urls()
        new_urls: UrlDetails = {
            old_loc.url: {
                'content_length': old_loc.size()
            }
            for old_url
            in old_urls
            for old_loc
            in [new_loc.file_in_this_directory(self._filename_of_url(old_url))]
        }
        new_directory.save_preliminary_manifest(new_urls)
        old_finalized_manifest = self.loc.file_in_this_directory('_manifest')
        if old_finalized_manifest.exists():
            new_directory.finalize_manifest()
        return new_directory

    def copy_from(self, old_loc: BaseDirectoryUrl) -> 'RecordsDirectory':
        old_directory = RecordsDirectory(records_loc=old_loc)
        return old_directory.copy_to(self.loc)

    def finalize_manifest(self) -> None:
        old_loc = self.loc.file_in_this_directory('manifest')
        new_loc = self.loc.file_in_this_directory('_manifest')
        if new_loc.exists():
            logger.info(f"Deleting {new_loc} and finalizing new manifest in its place")
            new_loc.delete()
        old_loc.rename_to(new_loc)
        new_loc.wait_to_exist()

    def get_manifest(self) -> LegacyRecordsManifest:
        """Note that this returns a 'LegacyRecordsManifest' type - the output
        is not guaranteed to contain file length entries, as entries
        may have been written out by older versions of Records Mover
        prior to the file lengths being added to the manifest.
        """
        manifest_loc = self.loc.file_in_this_directory('_manifest')
        if not manifest_loc.exists():
            manifest_loc = self.loc.file_in_this_directory('manifest')
        return manifest_loc.json_contents()  # type: ignore

    def manifest_entry_urls(self) -> List[str]:
        manifest = self.get_manifest()
        if manifest is None:
            raise TypeError('Manifest empty')
        else:
            return [entry['url'] for entry in manifest['entries']]

    def save_to_url(self, output_loc: BaseFileUrl) -> None:
        records_format = self.load_format(fail_if_dont_understand=True)
        manifest_entry_urls = self.manifest_entry_urls()
        if len(manifest_entry_urls) == 1:
            input_filename = self._filename_of_url(manifest_entry_urls[0])
            input_loc = self.loc.file_in_this_directory(input_filename)
            input_loc.copy_to(output_loc)
        else:
            if isinstance(records_format, DelimitedRecordsFormat):
                if records_format.hints['header-row']:
                    raise NotImplementedError("Please teach me how to concatenate "
                                              "delimited files with header row")
                else:
                    input_filenames = [self._filename_of_url(input_url)
                                       for input_url in manifest_entry_urls]
                    input_locs = [self.loc.file_in_this_directory(input_filename)
                                  for input_filename in input_filenames]
                    logger.info(f"Saving files from {self.loc.url} to {output_loc.url}")
                    output_loc.concatenate_from(input_locs)
            else:
                raise NotImplementedError("Please teach me how to concatenate this format of file")

    def await_completion(self,
                         manifest_filename: str = "_manifest",
                         log_level: int = logging.INFO,
                         ms_between_polls: int = 50) -> None:
        manifest_loc = self.loc.file_in_this_directory(manifest_filename)
        manifest_loc.wait_to_exist(log_level=log_level, ms_between_polls=ms_between_polls)
        manifest_locs = [self.loc.file_in_this_directory(self._filename_of_url(url))
                         for url in self.manifest_entry_urls()]
        for loc in manifest_locs:
            loc.wait_to_exist(log_level=log_level, ms_between_polls=ms_between_polls)

    def __str__(self) -> str:
        return f"{type(self).__name__}({self.loc.url})"
