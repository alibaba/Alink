from .common import OneVsRest as _OneVsRest


class OneVsRest(_OneVsRest):
    def setClassifier(self, classifier):
        self.get_j_obj().setClassifier(classifier.get_j_obj())
        return self
