from collections import namedtuple
import threading
from copy import deepcopy
from dbt.logger import CACHE_LOGGER as logger
import dbt.exceptions


_ReferenceKey = namedtuple('_ReferenceKey', 'database schema identifier')


def _lower(value):
    """Postgres schemas can be None so we can't just call lower()."""
    if value is None:
        return None
    return value.lower()


def _make_key(relation):
    """Make _ReferenceKeys with lowercase values for the cache so we don't have
    to keep track of quoting
    """
    return _ReferenceKey(_lower(relation.database),
                         _lower(relation.schema),
                         _lower(relation.identifier))


def dot_separated(key):
    """Return the key in dot-separated string form.

    :param key _ReferenceKey: The key to stringify.
    """
    return '.'.join(map(str, key))


class _CachedRelation:
    """Nothing about _CachedRelation is guaranteed to be thread-safe!

    :attr str schema: The schema of this relation.
    :attr str identifier: The identifier of this relation.
    :attr Dict[_ReferenceKey, _CachedRelation] referenced_by: The relations
        that refer to this relation.
    :attr BaseRelation inner: The underlying dbt relation.
    """
    def __init__(self, inner):
        self.referenced_by = {}
        self.inner = inner

    def __str__(self):
        return (
            '_CachedRelation(database={}, schema={}, identifier={}, inner={})'
        ).format(self.database, self.schema, self.identifier, self.inner)

    @property
    def database(self):
        return _lower(self.inner.database)

    @property
    def schema(self):
        return _lower(self.inner.schema)

    @property
    def identifier(self):
        return _lower(self.inner.identifier)

    def __copy__(self):
        new = self.__class__(self.inner)
        new.__dict__.update(self.__dict__)
        return new

    def __deepcopy__(self, memo):
        new = self.__class__(self.inner.incorporate())
        new.__dict__.update(self.__dict__)
        new.referenced_by = deepcopy(self.referenced_by, memo)

    def is_referenced_by(self, key):
        return key in self.referenced_by

    def key(self):
        """Get the _ReferenceKey that represents this relation

        :return _ReferenceKey: A key for this relation.
        """
        return _make_key(self)

    def add_reference(self, referrer):
        """Add a reference from referrer to self, indicating that if this node
        were drop...cascaded, the referrer would be dropped as well.

        :param _CachedRelation referrer: The node that refers to this node.
        """
        self.referenced_by[referrer.key()] = referrer

    def collect_consequences(self):
        """Recursively collect a set of _ReferenceKeys that would
        consequentially get dropped if this were dropped via
        "drop ... cascade".

        :return Set[_ReferenceKey]: All the relations that would be dropped
        """
        consequences = {self.key()}
        for relation in self.referenced_by.values():
            consequences.update(relation.collect_consequences())
        return consequences

    def release_references(self, keys):
        """Non-recursively indicate that an iterable of _ReferenceKey no longer
        exist. Unknown keys are ignored.

        :param Iterable[_ReferenceKey] keys: The keys to drop.
        """
        keys = set(self.referenced_by) & set(keys)
        for key in keys:
            self.referenced_by.pop(key)

    def rename(self, new_relation):
        """Rename this cached relation to new_relation.
        Note that this will change the output of key(), all refs must be
        updated!

        :param _CachedRelation new_relation: The new name to apply to the
            relation
        """
        # Relations store this stuff inside their `path` dict. But they
        # also store a table_name, and usually use it in their  .render(),
        # so we need to update that as well. It doesn't appear that
        # table_name is ever anything but the identifier (via .create())
        self.inner = self.inner.incorporate(
            path={
                'database': new_relation.inner.database,
                'schema': new_relation.inner.schema,
                'identifier': new_relation.inner.identifier
            },
            table_name=new_relation.inner.identifier
        )

    def rename_key(self, old_key, new_key):
        """Rename a reference that may or may not exist. Only handles the
        reference itself, so this is the other half of what `rename` does.

        If old_key is not in referenced_by, this is a no-op.

        :param _ReferenceKey old_key: The old key to be renamed.
        :param _ReferenceKey new_key: The new key to rename to.
        :raises InternalError: If the new key already exists.
        """
        if new_key in self.referenced_by:
            dbt.exceptions.raise_cache_inconsistent(
                'in rename of "{}" -> "{}", new name is in the cache already'
                .format(old_key, new_key)
            )

        if old_key not in self.referenced_by:
            return
        value = self.referenced_by.pop(old_key)
        self.referenced_by[new_key] = value

    def dump_graph_entry(self):
        """Return a key/value pair representing this key and its referents.

        return List[str]: The dot-separated form of all referent keys.
        """
        return [dot_separated(r) for r in self.referenced_by]


def lazy_log(msg, func):
    if logger.disabled:
        return
    logger.debug(msg.format(func()))


class RelationsCache:
    """A cache of the relations known to dbt. Keeps track of relationships
    declared between tables and handles renames/drops as a real database would.

    :attr Dict[_ReferenceKey, _CachedRelation] relations: The known relations.
    :attr threading.RLock lock: The lock around relations, held during updates.
        The adapters also hold this lock while filling the cache.
    :attr Set[str] schemas: The set of known/cached schemas, all lowercased.
    """
    def __init__(self):
        self.relations = {}
        self.lock = threading.RLock()
        self.schemas = set()

    def add_schema(self, database, schema):
        """Add a schema to the set of known schemas (case-insensitive)

        :param str database: The database name to add.
        :param str schema: The schema name to add.
        """
        self.schemas.add((_lower(database), _lower(schema)))

    def remove_schema(self, database, schema):
        """Remove a schema from the set of known schemas (case-insensitive)

        If the schema does not exist, it will be ignored - it could just be a
        temporary table.

        :param str database: The database name to remove.
        :param str schema: The schema name to remove.
        """
        self.schemas.discard((_lower(database), _lower(schema)))

    def update_schemas(self, schemas):
        """Add multiple schemas to the set of known schemas (case-insensitive)

        :param Iterable[str] schemas: An iterable of the schema names to add.
        """
        self.schemas.update((_lower(d), _lower(s)) for (d, s) in schemas)

    def __contains__(self, schema_id):
        """A schema is 'in' the relations cache if it is in the set of cached
        schemas.

        :param Tuple[str, str] schema: The db name and schema name to look up.
        """
        db, schema = schema_id
        return (_lower(db), _lower(schema)) in self.schemas

    def dump_graph(self):
        """Dump a key-only representation of the schema to a dictionary. Every
        known relation is a key with a value of a list of keys it is referenced
        by.
        """
        # we have to hold the lock for the entire dump, if other threads modify
        # self.relations or any cache entry's referenced_by during iteration
        # it's a runtime error!
        with self.lock:
            return {
                dot_separated(k): v.dump_graph_entry()
                for k, v in self.relations.items()
            }

    def _setdefault(self, relation):
        """Add a relation to the cache, or return it if it already exists.

        :param _CachedRelation relation: The relation to set or get.
        :return _CachedRelation: The relation stored under the given relation's
            key
        """
        self.add_schema(relation.database, relation.schema)
        key = relation.key()
        return self.relations.setdefault(key, relation)

    def _add_link(self, referenced_key, dependent_key):
        """Add a link between two relations to the database. Both the old and
        new entries must alraedy exist in the database.

        :param _ReferenceKey referenced_key: The key identifying the referenced
            model (the one that if dropped will drop the dependent model).
        :param _ReferenceKey dependent_key: The key identifying the dependent
            model.
        :raises InternalError: If either entry does not exist.
        """
        referenced = self.relations.get(referenced_key)
        if referenced is None:
            dbt.exceptions.raise_cache_inconsistent(
                'in add_link, referenced link key {} not in cache!'
                .format(referenced_key)
            )

        dependent = self.relations.get(dependent_key)
        if dependent is None:
            dbt.exceptions.raise_cache_inconsistent(
                'in add_link, dependent link key {} not in cache!'
                .format(dependent_key)
            )

        referenced.add_reference(dependent)

    def add_link(self, referenced, dependent):
        """Add a link between two relations to the database. Both the old and
        new entries must already exist in the database.

        The dependent model refers _to_ the referenced model. So, given
        arguments of (jake_test, bar, jake_test, foo):
        both values are in the schema jake_test and foo is a view that refers
        to bar, so "drop bar cascade" will drop foo and all of foo's
        dependents.

        :param BaseRelation referenced: The referenced model.
        :param BaseRelation dependent: The dependent model.
        :raises InternalError: If either entry does not exist.
        """
        referenced = _make_key(referenced)
        if (referenced.database, referenced.schema) not in self:
            # if we have not cached the referenced schema at all, we must be
            # referring to a table outside our control. There's no need to make
            # a link - we will never drop the referenced relation during a run.
            logger.debug(
                '{dep!s} references {ref!s} but {ref.database}.{ref.schema} '
                'is not in the cache, skipping assumed external relation'
                .format(dep=dependent, ref=referenced)
            )
            return
        dependent = _make_key(dependent)
        logger.debug(
            'adding link, {!s} references {!s}'.format(dependent, referenced)
        )
        with self.lock:
            self._add_link(referenced, dependent)

    def add(self, relation):
        """Add the relation inner to the cache, under the schema schema and
        identifier identifier

        :param BaseRelation relation: The underlying relation.
        """
        cached = _CachedRelation(relation)
        logger.debug('Adding relation: {!s}'.format(cached))

        lazy_log('before adding: {!s}', self.dump_graph)

        with self.lock:
            self._setdefault(cached)

        lazy_log('after adding: {!s}', self.dump_graph)

    def _remove_refs(self, keys):
        """Removes all references to all entries in keys. This does not
        cascade!

        :param Iterable[_ReferenceKey] keys: The keys to remove.
        """
        # remove direct refs
        for key in keys:
            del self.relations[key]
        # then remove all entries from each child
        for cached in self.relations.values():
            cached.release_references(keys)

    def _drop_cascade_relation(self, dropped):
        """Drop the given relation and cascade it appropriately to all
        dependent relations.

        :param _CachedRelation dropped: An existing _CachedRelation to drop.
        """
        if dropped not in self.relations:
            logger.debug('dropped a nonexistent relationship: {!s}'
                         .format(dropped))
            return
        consequences = self.relations[dropped].collect_consequences()
        logger.debug(
            'drop {} is cascading to {}'.format(dropped, consequences)
        )
        self._remove_refs(consequences)

    def drop(self, relation):
        """Drop the named relation and cascade it appropriately to all
        dependent relations.

        Because dbt proactively does many `drop relation if exist ... cascade`
        that are noops, nonexistent relation drops cause a debug log and no
        other actions.

        :param str schema: The schema of the relation to drop.
        :param str identifier: The identifier of the relation to drop.
        """
        dropped = _make_key(relation)
        logger.debug('Dropping relation: {!s}'.format(dropped))
        with self.lock:
            self._drop_cascade_relation(dropped)

    def _rename_relation(self, old_key, new_relation):
        """Rename a relation named old_key to new_key, updating references.
        Return whether or not there was a key to rename.

        :param _ReferenceKey old_key: The existing key, to rename from.
        :param _CachedRelation new_key: The new relation, to rename to.
        """
        # On the database level, a rename updates all values that were
        # previously referenced by old_name to be referenced by new_name.
        # basically, the name changes but some underlying ID moves. Kind of
        # like an object reference!
        relation = self.relations.pop(old_key)
        new_key = new_relation.key()

        # relaton has to rename its innards, so it needs the _CachedRelation.
        relation.rename(new_relation)
        # update all the relations that refer to it
        for cached in self.relations.values():
            if cached.is_referenced_by(old_key):
                logger.debug(
                    'updated reference from {0} -> {2} to {1} -> {2}'
                    .format(old_key, new_key, cached.key())
                )
                cached.rename_key(old_key, new_key)

        self.relations[new_key] = relation
        # also fixup the schemas!
        self.remove_schema(old_key.database, old_key.schema)
        self.add_schema(new_key.database, new_key.schema)

        return True

    def _check_rename_constraints(self, old_key, new_key):
        """Check the rename constraints, and return whether or not the rename
        can proceed.

        If the new key is already present, that is an error.
        If the old key is absent, we debug log and return False, assuming it's
        a temp table being renamed.

        :param _ReferenceKey old_key: The existing key, to rename from.
        :param _ReferenceKey new_key: The new key, to rename to.
        :return bool: If the old relation exists for renaming.
        :raises InternalError: If the new key is already present.
        """
        if new_key in self.relations:
            dbt.exceptions.raise_cache_inconsistent(
                'in rename, new key {} already in cache: {}'
                .format(new_key, list(self.relations.keys()))
            )

        if old_key not in self.relations:
            logger.debug(
                'old key {} not found in self.relations, assuming temporary'
                .format(old_key)
            )
            return False
        return True

    def rename(self, old, new):
        """Rename the old schema/identifier to the new schema/identifier and
        update references.

        If the new schema/identifier is already present, that is an error.
        If the schema/identifier key is absent, we only debug log and return,
        assuming it's a temp table being renamed.

        :param BaseRelation old: The existing relation name information.
        :param BaseRelation new: The new relation name information.
        :raises InternalError: If the new key is already present.
        """
        old_key = _make_key(old)
        new_key = _make_key(new)
        logger.debug('Renaming relation {!s} to {!s}'.format(
            old_key, new_key
        ))

        lazy_log('before rename: {!s}', self.dump_graph)

        with self.lock:
            if self._check_rename_constraints(old_key, new_key):
                self._rename_relation(old_key, _CachedRelation(new))
            else:
                self._setdefault(_CachedRelation(new))

        lazy_log('after rename: {!s}', self.dump_graph)

    def get_relations(self, database, schema):
        """Case-insensitively yield all relations matching the given schema.

        :param str schema: The case-insensitive schema name to list from.
        :return List[BaseRelation]: The list of relations with the given
            schema
        """
        schema = _lower(schema)
        with self.lock:
            results = [
                r.inner for r in self.relations.values()
                if (r.schema == _lower(schema) and
                    r.database == _lower(database))
            ]

        if None in results:
            dbt.exceptions.raise_cache_inconsistent(
                'in get_relations, a None relation was found in the cache!'
            )
        return results

    def clear(self):
        """Clear the cache"""
        with self.lock:
            self.relations.clear()
            self.schemas.clear()
