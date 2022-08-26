from keras import backend as K
from tensorflow import keras
from tensorflow.keras.applications import VGG16, ResNet50
from tensorflow.keras.layers import (
    Activation,
    BatchNormalization,
    Concatenate,
    Conv2D,
    Conv2DTranspose,
    Dropout,
    MaxPooling2D,
    UpSampling2D,
)


def mini_unet(n_classes=1, input_height=512, input_width=512, channels=2):
    inputs = keras.Input(shape=(input_height, input_width, channels))

    conv1 = Conv2D(32, (3, 3), padding="same", activation="relu")(inputs)
    conv1 = Dropout(0.2)(conv1)
    conv1 = Conv2D(32, (3, 3), padding="same", activation="relu")(conv1)
    pool1 = MaxPooling2D((2, 2))(conv1)

    conv2 = Conv2D(64, (3, 3), activation="relu", padding="same")(pool1)
    conv2 = Dropout(0.2)(conv2)
    conv2 = Conv2D(64, (3, 3), activation="relu", padding="same")(conv2)
    pool2 = MaxPooling2D((2, 2))(conv2)

    conv3 = Conv2D(128, (3, 3), activation="relu", padding="same")(pool2)
    conv3 = Dropout(0.2)(conv3)
    conv3 = Conv2D(128, (3, 3), activation="relu", padding="same")(conv3)

    up1 = Concatenate()([UpSampling2D((2, 2))(conv3), conv2])
    conv4 = Conv2D(64, (3, 3), activation="relu", padding="same")(up1)
    conv4 = Dropout(0.2)(conv4)
    conv4 = Conv2D(64, (3, 3), activation="relu", padding="same")(conv4)

    up2 = Concatenate()([UpSampling2D((2, 2))(conv4), conv1])
    conv5 = Conv2D(32, (3, 3), activation="relu", padding="same")(up2)
    conv5 = Dropout(0.2)(conv5)
    conv5 = Conv2D(32, (3, 3), activation="relu", padding="same")(conv5)

    outputs = Conv2D(n_classes, (1, 1), activation="sigmoid", padding="same")(conv5)

    model = keras.Model(inputs, outputs)
    return model


def conv_block(input, num_filters):
    x = Conv2D(num_filters, 3, padding="same")(input)
    x = BatchNormalization()(x)
    x = Activation("relu")(x)

    x = Conv2D(num_filters, 3, padding="same")(x)
    x = BatchNormalization()(x)
    x = Activation("relu")(x)
    return x


def decoder_block(input, skip_features, num_filters):
    x = Conv2DTranspose(num_filters, (2, 2), strides=2, padding="same")(input)
    x = Concatenate()([x, skip_features])
    x = conv_block(x, num_filters)
    return x


def vgg16_unet(n_classes=1, input_height=512, input_width=512, channels=2):
    # Input
    inputs = keras.Input(shape=(input_height, input_width, channels))

    # Pre-trained VGG16 Model
    vgg16 = VGG16(include_top=False, weights=None, input_tensor=inputs)

    # Encoder
    s1 = vgg16.get_layer("block1_conv2").output
    s2 = vgg16.get_layer("block2_conv2").output
    s3 = vgg16.get_layer("block3_conv3").output
    s4 = vgg16.get_layer("block4_conv3").output

    # Bridge
    b1 = vgg16.get_layer("block5_conv3").output

    # Decoder
    d1 = decoder_block(b1, s4, 512)
    d2 = decoder_block(d1, s3, 256)
    d3 = decoder_block(d2, s2, 128)
    d4 = decoder_block(d3, s1, 64)

    # Output
    outputs = Conv2D(n_classes, 1, padding="same", activation="sigmoid")(d4)

    model = keras.Model(inputs, outputs, name="VGG16_U-Net")
    return model


def resnet50_unet(n_classes=1, input_height=512, input_width=512, channels=2):
    # Input
    inputs = keras.Input(shape=(input_height, input_width, channels))

    # Pre-trained ResNet50 Model
    resnet50 = ResNet50(include_top=False, weights=None, input_tensor=inputs)

    # Encoder
    s1 = resnet50.get_layer("input_1").output
    s2 = resnet50.get_layer("conv1_relu").output
    s3 = resnet50.get_layer("conv2_block3_out").output
    s4 = resnet50.get_layer("conv3_block4_out").output

    # Bridge
    b1 = resnet50.get_layer("conv4_block6_out").output

    # Decoder
    d1 = decoder_block(b1, s4, 512)
    d2 = decoder_block(d1, s3, 256)
    d3 = decoder_block(d2, s2, 128)
    d4 = decoder_block(d3, s1, 64)

    # Output
    outputs = Conv2D(n_classes, 1, padding="same", activation="sigmoid")(d4)

    model = keras.Model(inputs, outputs, name="VGG16_U-Net")
    return model


def dice_coef(y_true, y_pred):
    smooth = 1.0
    y_true_f = K.flatten(y_true)
    y_pred_f = K.flatten(y_pred)
    intersection = K.sum(y_true_f * y_pred_f)
    return (2.0 * intersection + smooth) / (K.sum(y_true_f) + K.sum(y_pred_f) + smooth)


def dice_coef_loss(y_true, y_pred):
    return 1.0 - dice_coef(y_true, y_pred)
