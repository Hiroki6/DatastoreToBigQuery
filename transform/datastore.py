from apache_beam import PTransform, DoFn

class Kind:
    def __init__(self, name, id):
        self.name = name
        self.id = id

def get_kind(entity):
    # If the entity has a parent entity, that entity information is contained on the latter part of the path_elements.
    if len(entity.key.path_elements) > 2:
        kind = entity.key.path_elements[2]
        id_or_name = entity.key.path_elements[3]
    else:
        kind = entity.key.path_elements[0]
        id_or_name = entity.key.path_elements[1]

    return Kind(kind, id_or_name)

def convert(entity):
    """

    :param entity: Entity of Datastore
    :return: Dictionary of Entity
    """
    from datetime import datetime
    from google.cloud.datastore.helpers import GeoPoint

    kind = get_kind(entity)

    return dict(
        {k: isinstance(v, datetime) and str(v)
            or isinstance(v, GeoPoint) and {'lat': str(v.latitude), 'long': str(v.longitude)}
            or v
         for k, v in entity.properties.items()},
        __key__={
            'name': isinstance(kind.id, str) and kind.id or None,
            'id': isinstance(kind.id, int) and kind.id or None,
            'kind': kind.name,
            'namespace': entity.key.namespace,
            'path': ','.join([isinstance(e, str) and '"{}"'.format(e) or str(e) for e in entity.key.path_elements])
        }
    )

class CreateQuery(DoFn):
    """
    Create a query for getting all entities the kind taken.
    """
    def __init__(self, project_id):
        self.project_id = project_id

    def process(self, element):
        """

        :param element: a kind name
        :return: [Query]
        """
        from apache_beam.io.gcp.datastore.v1new.types import Query
        return [Query(kind=element, project=self.project_id)]

class GetKinds(PTransform):
    """
    Get all Kind names
    """
    def __init__(self, project_id):
        self.project_id = project_id
        pass

    def expand(self, pcoll):
        """

        :return: PCollection[kind_name]
        """
        from google.cloud import datastore
        from apache_beam import Create
        import logging
        query = datastore.Client(self.project_id).query(kind='__kind__')
        query.keys_only()
        kinds = [entity.key.id_or_name for entity in query.fetch()]
        logging.info("kinds: {}".format(kinds))
        return pcoll.pipeline | 'Kind' >> Create(kinds)
