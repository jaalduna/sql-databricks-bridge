# Tasks

## Backlog
- [ ] Cargar sql-databricks-bridge en el server | priority:high | tags:deploy,server
- [ ] Probar ejecutable en windows + front end | priority:medium | tags:windows,test
- [ ] Front conectado a tabla en databricks | priority:medium | tags:frontend,databricks
  Dev token por ahora -> service principal
- [ ] Implement sync trigger with one command | priority:low | tags:feature,cli
- [ ] Implementar endpoint para ejecutar jobs con parametros | priority:medium | tags:feature,api
- [ ] Implementar parametro para saber quien hizo una solicitud en la api | priority:low | tags:feature,api
- [ ] Evaluar hacer el delta en bridge y solo upload de incremento | priority:medium | tags:optimization
- [ ] Validar que los tags se esten implementando | priority:medium | tags:validation,delta
- [ ] Ver donde se esta guardando el estado de las tareas | priority:medium | tags:frontend,m1
- [ ] Add details view of a particular query after click | priority:medium | tags:frontend,feature,m1
  Specially useful for debug failures
- [ ] Define what should be done when a failure occurs | priority:medium | tags:frontend,m1
  May be retry 3 times?
- [ ] Check if queries can be downloaded in parallel | priority:medium | tags:frontend,optimization,m1
- [ ] Provide the app to Jorge | priority:high | tags:frontend,m2
  Milestone 2 - 23 febrero

## Todo
- [ ] Revisar como correr el server con el ejecutable | priority:high | tags:server,deploy
- [ ] sql-databricks-bridge corriendo en servidor | priority:high | tags:deploy,server
- [ ] Habilitar para Mexico y Ecuador | priority:high | tags:frontend,m1
- [ ] steps and subteps information | priority:medium
  search for an endpoint that provide information from each step of the calibration process, such as sync to databricks 000-sql-databricks-bridge.{country} schema and then from there copy to 001-calibration-3-0.bronze-{country} schema and then -> merge -> simulate weights if not available -> simulate kpis for all/bc/original and finally calculate penetration and volume targets. 
  
  Objetives:
  implement a solution to retrieve information from each job status periodically, tipically using databricks sdk or other method u think is appropiate. Data should flow to the endpoint to be forwarded to the ../calibration-fronted application, Check the dev server there if u need more information on how the endpoint should work.
  
  For the tests use firt a mockup of databricks feedback, to validate internal data flow and then test with a subset for 1 product of bolivia. For the sync sql->databricks , use only 5 products from Bolivia. 
  
  use an agent team of 1 data_engineers, 1 backend engineer,  1 data_scientist, 1 code reviewer, 1 tester and 1 software architect to verify code organization is correct and organized. 
  
  Expected results:
  endpoint to feed calibration-backend tested and validated.
  
  note: 
  - dont wait for approvals, just run until u solve this if u cant solve a particular issue after 5 iterations, asamble a team of 5 engineers that find the root cause discusing and comparing hipotesis. 
  - teams coordinators should not solve problems, just plan and delegate tasks.

## In Progress

## Review
- [-] Merge kpioe queries to databricks 000 | priority:high | tags:queries,databricks
- [-] Jobs with partial failed queries should not be marked as complete | priority:high | tags:frontend,bug,m1
  Should show a warning instead
- [-] Agregar tags a las versiones historicas de Delta tables | priority:medium | tags:feature,delta

## Done
- [x] Compilar en ejecutable en windows | priority:medium | tags:build,windows
- [x] Compile front end with tauri in github actions | priority:medium | tags:ci,tauri
- [x] Incorporar query mordom para simulador de pesaje | priority:medium | tags:queries
- [x] Agregar Inicio en stage | priority:medium | tags:frontend,m1
- [x] Validation with Colombia sync | priority:high | tags:frontend,colombia,m1
- [x] Duration always showing 0ms | priority:high | tags:frontend,bug,m1
- [x] Correr aplicativo para extraer Bolivia | priority:medium | tags:bolivia,extract
