from lxml.html import fromstring
from lxml import etree
import lxml
import logging

from .transformer import Transformer


class HtmlParser(Transformer):
    """Transformation for parsing a string html document to an traversable html tree.

    This class handles the parsing of plaintext html to a traversable tree format, for each record the plaintext html
    is parsed and the metadata is updated.
    """
    @staticmethod
    def parse_html(data):
        """Parse an html string using the html.parser parser.

        The parsing is done using the lxml library. The parser uses some normalization, which should be fine, since
        it doesn't change the underlying structure of the html.

        Args:
            data(str): Html string

        Returns:
            etree: Lxml traversable etree object
            list: all errors that appeared during the html parsing.

        .. _Lxml Python Library:
            http://lxml.de/
        """
        errors = []
        tree = None
        try:
            tree = fromstring(data)
        except lxml.etree.ParserError as e:
            logging.error('HtmlParser - ' + str(e))
            errors.append('HtmlParser: ' + str(e))
        except AttributeError as e:
            logging.error('HtmlParser - ' + str(e))
            errors.append('HtmlParser: ' + str(e))
        return tree, errors

    def run(self, record):
        """Runs the HtmlPArse transformation on a record.

        This method applies the TreeParse transformation on the plain html data contained in the record. And logs
        the applied transformation for this record.

        Args:
            record(dict): Record containing the data that is transformed in this transformation step and metadata
                that is to be updated.

        Returns:
            dict: Updated record with the data being the traversable html tree and
            the metadata containing new errors and the executed transformation step.
        """
        logging.info("HtmlParser -  parsing: {0}".format(record['url']))
        self.check_required_transformations(record['metadata']['transformations'])

        parsed_html, errors = HtmlParser.parse_html(record['data'])
        record['data'] = parsed_html

        record['metadata']['errors'].extend(errors)
        record['metadata']['transformations'].append(self.output_transformation)

        return record

