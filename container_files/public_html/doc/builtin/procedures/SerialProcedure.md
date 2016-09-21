# Serial Procedure

The serial procedure type is used to configure a series of other procedure steps to
run one after the other.  It is the primary means by which procedures can be
composed.

## Execution

Each procedure step that is configured will be executed in turn, with all entities
and artifacts that are created in previous steps available to later steps.

## Errors

In the case that a procedure step has an error, the entire procedure will stop
training after the erroneous step.

## Configuration

![](%%config procedure serial)

and the step configuration key-value definitions are:

![](%%type MLDB::ProcedureStepConfig)


## See also

* [Procedures] (Procedures.md) for a description of procedures and what they can do.
