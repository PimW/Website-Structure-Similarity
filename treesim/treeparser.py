import logging

from .transformer import Transformer


class TreeParser(Transformer):
    """Class for parsing a traversable lxml tree to an compact and easy to process tree-like structure

    This class handles the parsing of traversable treeformat to a compact recursive array representation.
    for each record the traversable tree is parsed and the metadata is updated.

    Attributes:
        self.relevant_attributes(list): Attributes such as name, id, class
            that will be added to the attributes array
        self.filter_tags(list): The tags that will be filtered from the output, such that they are not in the
            output structure. Generally these are tags that don't mean anything for the structure, e.g. scriot tags.
        self.replacement_dict(dict): Replaces tag names for a more efficient representation if necessary.
            Uses the form {<tagname>:<replacement>}

    Args:
        requirements(list): Transformations that are required before executing the new transformation.
            For this class the requirements should generally be empty.
        output_transformation(str): label of the transformation that is applied in this object
    """
    def __init__(self, relevant_attributes, filter_tags, replacement_dict, requirements, output_transformation):
        super().__init__(requirements, output_transformation)

        # include relevant attributes such as names and classes
        self.relevant_attributes = relevant_attributes
        # filter non structural tags from given tag list
        self.filter_tags = filter_tags
        # replace tag names with a more efficient representation
        self.replacement_dict = replacement_dict

    def parse(self, tree, depth=0):
        """Recursive algorithm to parse a traversable tree to an easy to handle format.

        This recursive algorithm extracts the relevant data from each html tree element and then, executes
        this method recursively for each of it's children. For each elements the tagname is extracted
        and replaced (if necessary) unless it is filtered, the relevant attributes are extracted and
        stored in an array, finally recursively all the children are stored in the children array.
        If a specific child tag is filtered the children of that tag will be added to the current tag. This
        means that the subtree is not completely removed.

        Args:
            tree(xml tree): Traversable tree or subtree used as base for the algorithm.
            depth(int): current depth of the algorithm.

        Returns:
            tuple: Tuple containing the (replaced) tagname, array with attributes and array with all children.
                The children have a similar structure (<tag>, <attributes>, [child1, child2])
            list: all errors that appeared during the Tree Parsing, all errors are cascaded up the tree.
        """
        newtag = None
        attr_array = []
        errors = []

        # Filter unwanted tags
        if tree is None:
            errors.append('TreeParser: NoneType object')
            return (None, None, []), errors
        if tree.tag not in self.filter_tags and type(tree.tag) == str:
            # Find replacement tag otherwise keep old tag
            newtag = self.replacement_dict.get(tree.tag, tree.tag)

            # Find relevant attributes
            for attr in self.relevant_attributes:
                attr_val = tree.get(attr)
                if attr_val:
                    attr_array.append((attr, attr_val))

        child_array = []
        for child in tree:
            child_tuple, child_errors = self.parse(child, depth + 1)
            errors.extend(child_errors)

            # If the node is filtered still add the child nodes of that node to this node
            if not child_tuple[0]:
                child_array.extend(child_tuple[2])
            else:
                child_array.append(child_tuple)

        return (newtag, attr_array, child_array), errors

    def run(self, record):
        """Runs the TreeParse transformation on a record.

        This method applies the TreeParse transformation on the plain html data contained in the record.

        Args:
            record(dict): Record containing the data that is transformed in this transformation step and metadata
                that is to be updated.

        Returns:
            dict: Updated record with the data being the traversable html tree and
            the metadata containing new errors and the executed transformation step.
        """
        logging.warning("TreeParser -  parsing: {0}".format(record['url']))
        self.check_required_transformations(record['metadata']['transformations'])


        parsed_tree, errors = self.parse(record['data'])
        record['data'] = parsed_tree
        record['metadata']['errors'].extend(errors)
        record['metadata']['transformations'].append(self.output_transformation)
        return record
