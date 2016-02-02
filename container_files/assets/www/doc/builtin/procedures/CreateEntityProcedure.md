# Create Entity Procedure

The `createEntity` procedure type can create other entities as a procedure
step.  This is useful when views or similar need to be created as part
of a procedure running.

The createEntity procedure type can create all kinds of entities: procedures,
functions, datasets and plugins.

## Execution

When run, the procedure will have the same effect as a `POST` of the given
object to `/v1/<kind>s` endpoint.

## Errors

In the case of an error, the run will indicate that the step had an error.

## Configuration

![](%%config procedure createEntity)

## Output

The procedure's output will be in the following form, which allows for the
expanded configuration and the ID of the new entity to be obtained by the
caller:

![](%%type Datacratic::MLDB::CreateEntityProcedureOutput)


# See also

- [Procedures] (Procedures.md) for a description of procedures and what they can do.
- [Function Configuration] (../functions/FunctionConfig.md)
- [Dataset Configuration] (../datasets/DatasetConfig.md)
- [Plugin Configuration] (../plugins/PluginConfig.md)
- [Procedure Configuration] (../procedures/ProcedureConfig.md)
