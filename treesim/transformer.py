

class Transformer(object):
    """Base class for all transformer classes that are used in the pipeline.

    Provides an interface for the transformer classes. It contains the attributes that are required for each
    of the transformers. It also provides an not implemented interface for running the transformer and methods
    for linking transformers into a pipeline. The class also contains standard error checking.
    All transformers should inherit from this class.

    Args:
        requirements(list): Transformations that are required before executing the new transformation.
            Generally this should not be empty, except for transformations at the start of the pipeline.
        output_transformation(str): This should normally not be empty,
            except for in between analysis without data transformation steps.

    Attributes:
        self.required_transformations(list): Required transformations for this transformation in the pipeline.
        self.output_transformation(str): Label for the transformation that is executed using this transformer.

    Todo:
        * dependency management inside the transformers, free form data flow
        
    """
    def __init__(self, requirements, output_transformation):
        self.required_transformations = requirements
        self.output_transformation = output_transformation

    def run(self, record):
        """Default interface for running transformations.

        This function is empty and should be overridden in subclasses. It only describes the interface.
        
        Note:
            Not to be confused with execute, this is the transformation function, execute executes the pipeline
            up and including this transformation.

        Args:
            record: record that is to be transformed in the transformation step

        Returns:
            record: record after transformation that can be used in the next step in the pipeline.
            The record should be updated with the new data that can be used in the next step. The metadata
            containing the executed transformations and errors should also be updated.

        Raises:
            NotImplementedError: This function in it's basic state should never be executed.
        """
        raise NotImplementedError('run should be implemented in the subclass')

    def check_required_transformations(self, previous_transformations):
        """Checks whether all required transformations match.

        Args:
            previous_transformation: Transformation of the previous step in the pipeline.

        Returns:
            bool: True when all required transformations are available, False if a required transformation is missing.

        Raises:
            RuntimeError: when the required transformations and the input transformations
                don't match the pipeline should not be accepted.

        Todo:
            * Move runtime exception to actual code for connecting transformations

        """

        for requirement in self.required_transformations:
            if requirement not in previous_transformations:
                raise RuntimeError("Requirement '" + requirement + "' not met for transformation:  " + type(self).__name__
                                   + '\n' + str(previous_transformations))
