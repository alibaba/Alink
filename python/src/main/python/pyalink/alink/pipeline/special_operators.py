from .common import OneVsRest as _OneVsRest


class OneVsRest(_OneVsRest):
    def setClassifier(self, classifier):
        """
        Set classifier.

        :param classifier: a classifier.
        :return: `self`.
        """
        self.get_j_obj().setClassifier(classifier.get_j_obj())
        return self
