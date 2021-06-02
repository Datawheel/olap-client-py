"""Tests for the data models classes."""

import pytest
from olap_client.models import Cube, Dimension, Hierarchy, Level, Measure, Property


class TestCube:
    """Tests for the :class:`Cube` data model class."""

    def test_get_dimension(self, cube_instance: Cube):
        """Test for the `get_dimension` instance method."""
        dimension = cube_instance.get_dimension("Year")
        assert isinstance(dimension, Dimension)
        assert dimension.name == "Year"

    def test_get_dimension_raises(self, cube_instance: Cube):
        """Test for when the `get_dimension` method can't find the object."""
        with pytest.raises(StopIteration):
            cube_instance.get_dimension("invalid_name")

    def test_get_hierarchy(self, cube_instance: Cube):
        """Test for the `get_hierarchy` instance method."""
        hierarchy = cube_instance.get_hierarchy("HS92")
        assert isinstance(hierarchy, Hierarchy)
        assert hierarchy.name == "HS92"

    def test_get_hierarchy_raises(self, cube_instance: Cube):
        """Test for when the `get_hierarchy` method can't find the object."""
        with pytest.raises(StopIteration):
            cube_instance.get_hierarchy("invalid_name")

    def test_get_level(self, cube_instance: Cube):
        """Test for the `get_level` instance method."""
        level = cube_instance.get_level("Exporter Continent")
        assert isinstance(level, Level)
        assert level.name == "Continent"

    def test_get_level_raises(self, cube_instance: Cube):
        """Test for when the `get_level` method can't find the object."""
        with pytest.raises(StopIteration):
            cube_instance.get_level("invalid_name")

    def test_get_property(self, cube_instance: Cube):
        """Test for the `get_property` instance method."""
        propty = cube_instance.get_property("Importer Country ISO 2")
        assert isinstance(propty, Property)
        assert propty.name == "ISO 2"

    def test_get_property_raises(self, cube_instance: Cube):
        """Test for when the `get_property` method can't find the object."""
        with pytest.raises(StopIteration):
            cube_instance.get_property("invalid_name")

    def test_get_measure(self, cube_instance: Cube):
        """Test for the `get_measure` instance method."""
        measure = cube_instance.get_measure("Quantity")
        assert isinstance(measure, Measure)
        assert measure.name == "Quantity"

    def test_get_measure_raises(self, cube_instance: Cube):
        """Test for when the `get_measure` method can't find the object."""
        with pytest.raises(StopIteration):
            cube_instance.get_measure("invalid_name")


class TestDimension:
    """Tests for the :class:`Dimension` data model class."""

    def test_get_hierarchy(self, cube_instance: Cube):
        """Test for the `get_hierarchy` instance method."""
        dimension = cube_instance.dimensions[0]
        hierarchy = dimension.get_hierarchy("Year")
        assert isinstance(hierarchy, Hierarchy)
        assert hierarchy.name == "Year"

    def test_get_level(self, cube_instance: Cube):
        """Test for the `get_level` instance method."""
        dimension = cube_instance.dimensions[1]
        level = dimension.get_level("HS2")
        assert isinstance(level, Level)
        assert level.name == "HS2"

    def test_get_property(self, cube_instance: Cube):
        """Test for the `get_property` instance method."""
        dimension = cube_instance.dimensions[2]
        propty = dimension.get_property("Exporter Continent Name ES")
        assert isinstance(propty, Property)
        assert propty.name == "Continent ES"

    def test_get_hierarchy_raises(self, cube_instance: Cube):
        """Test for when the `get_hierarchy` method can't find the object."""
        dimension = cube_instance.dimensions[0]
        with pytest.raises(StopIteration):
            dimension.get_hierarchy("invalid_name")

    def test_get_level_raises(self, cube_instance: Cube):
        """Test for when the `get_level` method can't find the object."""
        dimension = cube_instance.dimensions[0]
        with pytest.raises(StopIteration):
            dimension.get_level("invalid_name")

    def test_get_property_raises(self, cube_instance: Cube):
        """Test for when the `get_property` method can't find the object."""
        dimension = cube_instance.dimensions[0]
        with pytest.raises(StopIteration):
            dimension.get_property("invalid_name")


class TestHierarchy:
    """Tests for the :class:`Hierarchy` data model class."""

    def test_matches(self, cube_instance: Cube):
        """Test for the `matches` instance method."""
        hierarchy = cube_instance.dimensions[0].hierarchies[0]
        assert hierarchy.matches("invalid_name") is False
        assert hierarchy.matches("Year") is True

    def test_get_level(self, cube_instance: Cube):
        """Test for the `get_level` instance method."""
        hierarchy = cube_instance.dimensions[1].hierarchies[0]
        level = hierarchy.get_level("HS4")
        assert isinstance(level, Level)
        assert level.name == "HS4"

    def test_get_property(self, cube_instance: Cube):
        """Test for the `get_property` instance method."""
        hierarchy = cube_instance.dimensions[2].hierarchies[0]
        propty = hierarchy.get_property("Exporter Country ISO 3")
        assert isinstance(propty, Property)
        assert propty.name == "ISO 3"

    def test_get_level_raises(self, cube_instance: Cube):
        """Test for when the `get_level` method can't find the object."""
        hierarchy = cube_instance.dimensions[1].hierarchies[0]
        with pytest.raises(StopIteration):
            hierarchy.get_level("invalid_name")

    def test_get_property_raises(self, cube_instance: Cube):
        """Test for when the `get_property` method can't find the object."""
        hierarchy = cube_instance.dimensions[2].hierarchies[0]
        with pytest.raises(StopIteration):
            hierarchy.get_property("invalid_name")


class TestLevel:
    """Tests for the :class:`Level` data model class."""

    def test_matches(self, cube_instance: Cube):
        """Test for the `matches` instance method."""
        level = cube_instance.dimensions[0].hierarchies[0].levels[0]
        assert level.matches("invalid_name") is False
        assert level.matches("Year") is True

    def test_get_property(self, cube_instance: Cube):
        """Test for the `get_property` instance method."""
        level = cube_instance.dimensions[3].hierarchies[0].levels[1]
        propty = level.get_property("Importer Country ID Number")
        assert isinstance(propty, Property)
        assert propty.name == "ID Number"


class TestProperty:
    """Tests for the :class:`Property` data model class."""

    def test_matches(self, cube_instance: Cube):
        """Test for the `matches` instance method."""
        propty = cube_instance.dimensions[3].hierarchies[0].levels[1].properties[3]
        assert propty.matches("invalid_name") is False
        assert propty.matches("Feenstra ID") is True
