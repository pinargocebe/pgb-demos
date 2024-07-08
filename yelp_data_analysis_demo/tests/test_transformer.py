import pytest

from app.exception import UnsupportedEntityException
from app.transformer import TransformerFactory


def test_get_unimplemented_transformer():
    with pytest.raises(UnsupportedEntityException) as exc:
        TransformerFactory.get_transformer("unimplemented_entity")

    assert str(exc.value) == "Implement transformer for entity: 'unimplemented_entity'"


# TODO add other tests
