import numpy as np
import pytest
from dags.transformation.etl_transforms import EtlTransforms
from dags.modelling.model import Model
import tensorflow as tf

np.random.seed(42)

@pytest.fixture
def random_data():
    X_train = np.random.randn(200, 27)
    y_train = np.random.randn(200, 1)
    X_validation = np.random.randn(50, 27)
    y_validation = np.random.randn(50, 1)
    return X_train, y_train, X_validation, y_validation

def test_dataset(random_data):
    """
    Test dataset creation from random data
    """
    X_train, y_train , _, _ = random_data
    train_dataset = EtlTransforms.build_dataset(x=X_train, y=y_train, sequence_length=30, batch_size=128)
    for x_batch, y_batch in train_dataset.take(1):
        assert x_batch.shape == (128, 30, 27)
        assert y_batch.shape == (128, 1)

def test_model_training(random_data):
    """
    Integration test for model training
    """
    X_train, y_train, X_validation, y_validation = random_data
    train_dataset = EtlTransforms.build_dataset(x=X_train, y=y_train, sequence_length=30, batch_size=128)
    validation_dataset = EtlTransforms.build_dataset(x=X_validation, y=y_validation, sequence_length=30, batch_size=128)
    model = Model()
    mdl = model.compile_model(time_steps=30)
    model._train(model=mdl, dataset=train_dataset, validation_dataset=validation_dataset, epochs=1)
