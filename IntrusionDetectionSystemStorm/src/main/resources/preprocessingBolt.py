import numpy as np
import pickle

import storm


class preprocessingBolt(storm.BasicBolt):
    # Load encoders for protocol type, service and flag
    label_encoder_flag = pickle.load(
        open("label_encoder_flag", 'rb'))
    label_encoder_prot = pickle.load(open("label_encoder_protocol_type", 'rb'))
    label_encoder_service = pickle.load(open("label_encoder_service", 'rb'))

    # Load the standard scaler for numerical features
    standard_scaler = pickle.load(open("standard_scaler", 'rb'))

    # Mark nominal, binary and numerical features
    nominal_idx, binary_idx = [1, 2, 3], [6, 11, 13, 14, 20, 21]
    numeric_idx = list(set(range(41)).difference(nominal_idx).difference(binary_idx))

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

    def process(self, tuple):
        # Encode categorical features: protocol type, service and flag
        protocol_type = tuple.values[preprocessingBolt.nominal_idx[0]]  # protocol_type
        protocol_type = preprocessingBolt.label_encoder_prot.transform([str(protocol_type)])[0]

        service = tuple.values[preprocessingBolt.nominal_idx[1]]  # service
        service = preprocessingBolt.label_encoder_service.transform([str(service)])[0]

        flag = tuple.values[preprocessingBolt.nominal_idx[2]]  # flag
        flag = preprocessingBolt.label_encoder_flag.transform([str(flag)])[0]

        # Scale numerical features
        scaled_features = preprocessingBolt.standard_scaler.transform(
            np.reshape([float(tuple.values[i]) for i in preprocessingBolt.numeric_idx], (1, -1)))[0]

        # Emit the tuple after processing
        storm.emit([scaled_features[0],
                    str(protocol_type),
                    str(service),
                    str(flag),
                    scaled_features[1],
                    scaled_features[2],
                    tuple.values[preprocessingBolt.binary_idx[0]],
                    scaled_features[3],
                    scaled_features[4],
                    scaled_features[5],
                    scaled_features[6],
                    tuple.values[preprocessingBolt.binary_idx[1]],
                    scaled_features[7],
                    tuple.values[preprocessingBolt.binary_idx[2]],
                    tuple.values[preprocessingBolt.binary_idx[3]],
                    scaled_features[8],
                    scaled_features[9],
                    scaled_features[10],
                    scaled_features[11],
                    scaled_features[12],
                    tuple.values[preprocessingBolt.binary_idx[4]],
                    tuple.values[preprocessingBolt.binary_idx[5]],
                    scaled_features[13],
                    scaled_features[14],
                    scaled_features[15],
                    scaled_features[16],
                    scaled_features[17],
                    scaled_features[18],
                    scaled_features[19],
                    scaled_features[20],
                    scaled_features[21],
                    scaled_features[22],
                    scaled_features[23],
                    scaled_features[24],
                    scaled_features[25],
                    scaled_features[26],
                    scaled_features[27],
                    scaled_features[28],
                    scaled_features[29],
                    scaled_features[30],
                    scaled_features[31]
                    ])


preprocessingBolt().run()
