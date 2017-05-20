import warc3
import logging
import urllib.request

from .transformer import Transformer

# LOGGING SETUP
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT, level=20)


class HtmlExtractor(Transformer):
    """Extracts Html and other relevant data from a warc file.

    This class handles the html extraction transformation in a pipeline. It uses
    a warc file to generate warc records. Those records can be parsed using this class.
    Either for each record separately or for all records at once. These records are put in a nice format and
    relevant extra information is added, including metadata that is used for information purposes in all
    steps in the pipeline. Once the records are processed they can be used in the rest of the pipeline.

    Args:
        requirements(list): Transformations that are required before executing the new transformation.
            for this class the requirements should generally be empty.
        output_transformation(str): label of the transformation that is applied in this object
        minlength(int): Minimum length for a html shard of a record to be considered. Default value = 5000

    Attributes:
        self.minlength(int): Minimum length for a html shard of a record to be considered. Default value = 5000

    Todo:
        * Rewrite to allow for a more flexible workflow
    """
    def __init__(self, requirements, output_transformation, minlength=10000, max_files=1000):
        super().__init__(requirements, output_transformation)

        self.minlength = minlength
        self.max_files = max_files

    @staticmethod
    def extract_warc_file(filename):
        """Opens given warc file

        Args:
            filename(str): Name of the warcfiles to be opened

        Returns:
            warc file: warc file iterable containing all warc records
        """
        return warc3.open(filename)

    def parse_warc_records(self, file, batch_size=100):
        """Extract all records from the warc file iterable.

        Extract all records from the given warc file iterable.
        Records that don't have type response or have a short content length are filtered.
        Records with other types contain headers and other metadata but no html, if
        the content is too short it is often just a redirect page.

        Args:
            file: File iterable containing all warc objects
            batch_size: Size of the batch that is extracted each time.

        Returns:
            list: List of parsed records from the file
        """
        results = []

        total_file_count = 0
        file_count = 0
        for record in file:
            if total_file_count + file_count >= self.max_files:
                yield results  # max files reached
                return

            if file_count >= batch_size:
                total_file_count += file_count
                res = results
                results = []
                file_count = 0
                yield res  # break

            parsed_record = self.parse_warc_record(record)
            if parsed_record:
                results.append(parsed_record)
                file_count += 1
        yield results  # batch_size is bigger than remaining file size

    def parse_warc_record(self, record):
        """Parse a single warc record.

        Parse a single warc record, relevant values from the object are extracted
        and put in a dictionary, extra fields are added to keep track of the transformation
        progress and errors that happen during pipeline execution.
        The url contains the actual web url. The data is the data that is used for each step in the pipeline,
        in this case a html bytes object, the metadata contains the transformations executed on a record and
        the errors that happened during processing.

        Args:
            record: A single warc record containing the full content from the archive.

        Returns:
            dict: containing the record information.

        Example:
            record = {
                'url': <url>,
                'data': <data>,
                'metadata': {
                    'transformations': [...],
                    'errors': []
                }
            }

        """
        if record['WARC-type'] == 'response' and int(record['Content-Length']) > self.minlength:
            record_dict = dict()
            record_dict['url'] = record['WARC-target-URI']

            html_str = record.payload.read()
            (response_headers, html_str) = html_str.split(b'\r\n\r\n', 1)

            if b'301 Moved Permanently' in response_headers:
                return

            record_dict['data'] = html_str.strip()
            record_dict['metadata'] = {
                'transformations': [self.output_transformation],
                'errors': []
            }  # TODO: add extra metadata from file where necessary
            return record_dict

    def record_from_url(self, url):
        with urllib.request.urlopen(url) as response:
            html = response.read()

        record_dict = dict()
        record_dict['url'] = url
        record_dict['data'] = html
        record_dict['metadata'] = {
            'transformations': [self.output_transformation],
            'errors': []
        }  # TODO: add extra metadata from file where necessary
        return record_dict

    def run(self, record):
        """Executes the transformation for a record.

        Is generally executed for each record in the pipeline. It is mostly used as a standardized interface.
        Overridden from the Transformer class. This method also logs the transformation
        for debugging/analysis purposes.

        Args:
            record: warc record

        Returns:
            dict: Transformed record with all relevant data and metadata in the dict format.
        """
        logging.info("HtmlExtractor -  extracting")

        return self.parse_warc_record(record)
