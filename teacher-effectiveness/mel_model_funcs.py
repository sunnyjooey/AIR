import numpy as np
from keras.utils import Sequence
from keras.applications.mobilenet_v2 import MobileNetV2
from keras.layers import Input, BatchNormalization, Conv2D, Dense, GlobalAveragePooling2D
from keras.models import Model
from keras.optimizers import Adam
from utils import uni_len, mel_0_1
import os, ssl
if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
    getattr(ssl, '_create_unverified_context', None)):
    ssl._create_default_https_context = ssl._create_unverified_context

class TestGenerator(Sequence):
    def __init__(self,
                 mel_files,
                 req_mel_len,
                 batch_size=64,
                 mel_data=None):

        self.mel_files = mel_files
        self.batch_size = batch_size
        self.mel_data = mel_data
        self.req_mel_len = req_mel_len


        self.one_set_size = int(np.ceil(len(self.mel_files) / self.batch_size))

        self.on_epoch_end()

    def load_one_mel(self, filename):
        x = self.mel_data[filename].copy()
        x = uni_len(x, self.req_mel_len)
        x = x[..., np.newaxis]
        return x

    def load_mels_for_batch(self, filelist):
        this_batch_data = [self.load_one_mel(x) for x in filelist]
        return np.array(this_batch_data)

    def __len__(self):
        return self.one_set_size

    def __getitem__(self, index):
        return self.__data_generation(index)

    def on_epoch_end(self):
        # initialize the indices
        self.indexes = np.arange(len(self.mel_files))

        # create mel array(s)
        tmp = []
        for one_req_len in [self.req_mel_len]:
            self.req_mel_len = one_req_len
            tmp.append(self.load_mels_for_batch([
                  self.mel_files[i] for i in np.arange(len(self.mel_files))
            ]))
        self.mel_this_epoch = tmp

    def __data_generation(self, batch_num):

        this_set = int(batch_num / self.one_set_size)
        this_index = batch_num % self.one_set_size
        this_indices = self.indexes[this_index*self.batch_size:(this_index+1)*self.batch_size]

        this_batch_mel = self.mel_this_epoch[this_set][this_indices, :, :]

        return this_batch_mel


def create_mel_model():
    mn = MobileNetV2(include_top=False)
    mn.layers.pop(0)
    inp = Input(shape=(64, None, 1))
    x = BatchNormalization()(inp)
    x = Conv2D(10, kernel_size=(1, 1), padding='same', activation='relu')(x)
    x = Conv2D(3, kernel_size=(1, 1), padding='same', activation='relu')(x)
    mn_out = mn(x)
    x = GlobalAveragePooling2D()(mn_out)
    x = Dense(1536, activation='relu')(x)
    x = BatchNormalization()(x)
    x = Dense(384, activation='relu')(x)
    x = BatchNormalization()(x)
    x = Dense(41, activation='softmax')(x)
    model = Model(inputs=[inp], outputs=x)
    model.compile(loss='categorical_crossentropy',
                  optimizer=Adam(lr=0.0001),
                  metrics=['accuracy'])
    return model
