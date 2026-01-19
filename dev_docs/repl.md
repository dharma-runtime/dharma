HELP

IDENTITY
-----------------
id init <name> <email> <password>           Initialize identity
id status                                   Show identity status
id unlock <password>                        Unlock identity
id lock                                     Lock identity
id whoami                                   Show identity information
id export                                   Export identity

ALIAS
-----------------
alias set <name> <value>                          Set alias
alias rm <name>                                   Remove alias
alias list                                        List aliases

CONFIG
-----------------
conf show                                       Show resolved config

CONTRACTS
-----------------
ct ls                                             List contracts
ct info <contract>                                Show contract information
ct schema <contract>                              Show contract schema
ct actions <contract>                             Show contract actions
ct reactors <contract>                            Show contract reactors

SUBJECTS
-----------------
ls                                                List subjects         
ls recent                                         List recent subjects        
ls mine                                           List subjects owned by current identity
ls c <contract>                                   List subjects for contract    

SUBJECT
-----------------
new <contract>                                    Create subject for contract
do <Action> [k=v...]                              Perform an action on the subject
try <Action> [k=v...]                             Simulate an action
can                                               Show all allowed actions on subject and their arguments
can <Action> [k=v...]                             Check if action is allowed on subject
why                                               Explain the current state. Show history, who, what, when
why <path>                                        Explain a state field
prove                                             Prove latest assertion
prove <assertion_id>                              Prove a specific assertion
state                                             Show subject state
status                                            Show subject status
diff                                              Compare current and previous state
diff --at <idA> <idB>                             Compare two states

DB
-----------------
tables                                            List tables
table <table>                                     Show table information: fields, number of rows.
q <query pipeline>                                Execute a query pipeline
find "<query>"                                    Find rows matching a query
index [status|build|drop]                         Manage DHARMA-Q indexes
open <result_id_or_object_id>                     Open a search result

PACKAGE MANAGEMENT
-----------------
pkg ls                                            List packages
pkg search                                        Search for packages
pkg local                                         List local packages
pkg installed                                     List installed packages
pkg show <package_name>                           Show package information
pkg install <package_name>                        Install a package
pkg uninstall <package_name>                      Uninstall a package
pkg verify                                        Verify package integrity
pkg pin <package_name>                            Pin a package
pkg build <path>                                  Build a package
pkg publish <package_name>                        Publish a package

NETWORK
-----------------
peers [--json|--verbose]                          List known peers
connect <addr>                                    Connect + sync with peer
sync now | sync subject [id]                      Trigger sync

SESSION
-----------------
tail [n]                                          Show recent assertions
log [n]                                           Show verbose history
show <id> [--json|--raw]                          Show assertion/envelope
pwd                                               Show current context
version                                           Show build info
help                                              Show help
help <command>                                    Show help for a specific command
exit                                              Exit



identity [status|init|unlock|lock|whoami|export]  Manage identity
alias [set|rm|list]                               Manage aliases
subjects [recent|mine]                            List subjects
use <id|alias>                                    Set current subject
new <contract>                                    Create subject for contract
state [--json|--raw] [--at <id>] [--lens <ver>]    Show derived state
tail [n]                                          Show recent assertions
log [n]                                           Show verbose history
show <id> [--json|--raw]                          Show assertion/envelope
status [--verbose]                                Show subject status
contracts [schema|actions] <contract>             List/inspect contracts
dryrun action <Action> [k=v...]                   Simulate an action
commit action <Action> [k=v...]                   Commit an action
why <path>                                        Explain a state field
prove <id>                                        Full validation report
authority <Action> [k=v...]                       Authority explanation
diff --at <idA> <idB>                              Compare two states
pkg <list|show|install|verify|pin|remove>         Package management
overlay <status|list|enable|disable|show>         Overlay view controls
peers [--json|--verbose]                          List known peers
connect <addr>                                    Connect + sync with peer
sync now | sync subject [id]                      Trigger sync
discover [status|on|off]                          Toggle LAN discovery
index [status|build|drop]                         Manage DHARMA-Q indexes
find "<query>" [--limit n]                       Text search (DHARMA-Q)
q <query pipeline>                                DHARMA-Q query
open <result_id_or_object_id>                     Open a search result
pwd                                               Show current context
:set <key> <val>                                  Set REPL options
version                                           Show build info
exit                                              Exit
