import logging
import numpy

from .transformer import Transformer


class TreeFormatter(Transformer):
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
    def __init__(self, tags_dict,  requirements, output_transformation):
        super().__init__(requirements, output_transformation)

        self.tags_dict = tags_dict

        self.dimension = len(self.tags_dict) + 1
        self.matrix = numpy.zeros((self.dimension, self.dimension, self.dimension), dtype=numpy.int)
        self.qgrams = set()

    def parse(self, tree):
        parsed_tree, errors = self.parse_tree(tree, None)
        #return self.matrix.flatten(), errors
        return self.matrix.flatten(), self.qgrams, errors

    def parse_tree(self, tree, right_tree):
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

        # TODO: change documentation

        node = tree[0]
        # Filter unwanted tags
        if tree is None:
            errors.append('BinaryTreeFormatter: NoneType object')
            return (0, 0), errors

        new_left_node = 0
        new_right_node = 0

        if len(tree[2]) > 0:
            first_child = tree[2][0]
            other_children = tree[2][1:]

            new_left_node, errors = self.parse_tree(first_child, other_children)

        if right_tree and len(right_tree) > 0:
            first_child = right_tree[0]
            other_children = right_tree[1:]

            new_right_node, errors = self.parse_tree(first_child, other_children)


        """
        next_child = None
        if idx + 1 <= len(children):
            next_child = children[idx+1]
            next_child = (next_child[0], next_child[2])
        """
        idx1 = node
        idx2 = 0
        idx3 = 0
        if new_left_node:
            idx2 = new_left_node[0]
        if new_right_node:
            idx3 = new_right_node[0]

        str_index = (str(idx1), str(idx2), str(idx3))
        index = (idx1, idx2, idx3)

        old_val = self.matrix[idx1, idx2, idx3]
        self.matrix[idx1, idx2, idx3] = old_val + 1

        self.qgrams.add(".".join(str_index))
        binary_subtree = (new_left_node, new_right_node)

        return (node, binary_subtree), errors

    def print_recursive(self, tree, depth=0):
        # TODO: add documentation
        if tree and type(tree) == tuple:
            if type(tree[0]) == tuple:
                self.print_recursive(tree[0], depth + 1)
            else:
                print(" " * depth + str(tree[0]))
            if type(tree[1]) == tuple:
                self.print_recursive(tree[1], depth + 1)
            else:
                print(" " * depth + str(tree[1]))

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
        logging.info("BinaryTreeFormatter -  parsing: {0}".format(record['url']))
        self.check_required_transformations(record['metadata']['transformations'])

        matrix, qgrams, errors = self.parse(record['data'])
        record['data'] = {
            'matrix': matrix,
            'qgrams': qgrams
        }
        record['metadata']['errors'].extend(errors)
        record['metadata']['transformations'].append(self.output_transformation)

        self.matrix = numpy.zeros((self.dimension, self.dimension, self.dimension), dtype=numpy.int)
        self.qgrams = set()

        return record