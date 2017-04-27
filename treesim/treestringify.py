import logging

from .transformer import Transformer


class TreeStringifyTransformer(Transformer):
    """Class for
    
    
    Attributes:

    Args:

    """
    def __init__(self, requirements, output_transformation):
        super().__init__(requirements, output_transformation)

    def stringify(self, tree, depth=0):
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
        errors = []

        # Filter unwanted tags
        if not tree or not tree[0]:
            errors.append('TreeStringify: NoneType object')
            return "", errors

        tree_string = tree[0]
        #print(tree_string)
        for child in tree[2]:
            child_string, child_errors = self.stringify(child, depth + 1)
            errors.extend(child_errors)

            tree_string += child_string

        return tree_string, errors

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


        tree_string, errors = self.stringify(record['data'])
        record['data'] = tree_string
        record['metadata']['errors'].extend(errors)
        record['metadata']['transformations'].append(self.output_transformation)
        return record
