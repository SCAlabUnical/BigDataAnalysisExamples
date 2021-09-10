import pickle

import numpy as np
import storm


class modelBolt(storm.BasicBolt):
    # Load the SVM model from disk
    model = pickle.load(open("svm_model", 'rb'))

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

    def process(self, tuple):
        # Predict the connection type from a set of 23 types
        prediction = modelBolt.model.predict(np.reshape([int(tuple.values[0]),
                                                         str(tuple.values[1]),
                                                         str(tuple.values[2]),
                                                         str(tuple.values[3]),
                                                         int(tuple.values[4]),
                                                         int(tuple.values[5]),
                                                         int(tuple.values[6]),
                                                         int(tuple.values[7]),
                                                         int(tuple.values[8]),
                                                         int(tuple.values[9]),
                                                         int(tuple.values[10]),
                                                         int(tuple.values[11]),
                                                         int(tuple.values[12]),
                                                         int(tuple.values[13]),
                                                         int(tuple.values[14]),
                                                         int(tuple.values[15]),
                                                         int(tuple.values[16]),
                                                         int(tuple.values[17]),
                                                         int(tuple.values[18]),
                                                         int(tuple.values[19]),
                                                         int(tuple.values[20]),
                                                         int(tuple.values[21]),
                                                         int(tuple.values[22]),
                                                         int(tuple.values[23]),
                                                         float(tuple.values[24]),
                                                         float(tuple.values[25]),
                                                         float(tuple.values[26]),
                                                         float(tuple.values[27]),
                                                         float(tuple.values[28]),
                                                         float(tuple.values[29]),
                                                         float(tuple.values[30]),
                                                         int(tuple.values[31]),
                                                         int(tuple.values[32]),
                                                         float(tuple.values[33]),
                                                         float(tuple.values[34]),
                                                         float(tuple.values[35]),
                                                         float(tuple.values[36]),
                                                         float(tuple.values[37]),
                                                         float(tuple.values[38]),
                                                         float(tuple.values[39]),
                                                         float(tuple.values[40])], (1, -1)))[0]

        # Emit the predicted connection type
        storm.emit([int(prediction)])
        f = open("results.txt", "a")
        f.write(str(prediction) + "\n")
        f.close()


modelBolt().run()
