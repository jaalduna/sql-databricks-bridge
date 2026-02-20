# Tasks

## Backlog
- [ ] parametrizar visibilidad de pestañas del Front | priority:high
  Los usuarios solo deberian poder ver las pestañas que les atañe
  **Acceptance Criteria:**
  - [ ] Los usuarios solo pueden ver las pestañas que les atañe
- [ ] Implementar parametro para saber quien hizo una solicitud en la api | priority:low | tags:feature,api
- [ ] Los usuarios solo pueden ver las pestañas que les atañe | priority:medium
- [ ] Evaluar hacer el delta en bridge y solo upload de incremento | priority:medium | tags:optimization
- [ ] Ver donde se esta guardando el estado de las tareas | priority:medium | tags:frontend,m1
- [ ] Provide the app to Jorge | priority:high | tags:frontend,m2
  Milestone 2 - 23 febrero
- [ ] enable the option to sync particular queries | priority:medium

## Todo

## In Progress
- [-] Probar ejecutable en windows + front end | priority:medium | tags:windows,test
  **Acceptance Criteria:**
  - [ ] funciona el sync
  - [ ] funciona correr el proceso de MTR para Bolivia
- [-] Calibration work steps mark everything finished after couple of seconds of merge step run | priority:medium
  Calibration work steps mark everything finished after couple of seconds of merge step run.
- [-] Once calibration steps tasks are finished, ther card continue in working state. | priority:medium

## Review
- [-] add the option to skip copy data | priority:medium
- [-] funciona correr el proceso de MTR para Bolivia | priority:medium
- [-] funciona el sync | priority:medium

## Done
- [x] regarding the dates for month to calibrate, note that january is calibrated in february. | priority:medium
  Dates are not ok, If I select february, 2026 it shoud show no data, becouse for that month is not data ad rg_domicilios_pesos neither in mordom for that month.
- [x] Documentar App y compartir con fernando | priority:high | due:2026-02-18
  Documentar el front y el App para flujo de aprobación Numerator.
  La documentación debe tener al menos:
  - Especificar si el aplicativo es web o ejecutable de escritorio (es un desktop App)
  - Especificar las funcionalidades del aplicativo y como sería el flujo de trabajo de los usuarios, configuraciones, stack tecnológico, etc.
  crear una documentación separada para el front y otra para el backend. Utilizar diagramas para explicar los flujos, interacciones, estructura de datos, etc y todo lo que aplique y pueda ayudar a comprender mejor. Coordinar un agents team de 5 colaboradores.
- [x] keep calibration state | priority:medium
  When changing from page calibration to other page and come back all running processes states go back to the original (no state backup or persistant). The idea is that when going back to the calibration that the state of the calibration process are still there.
- [x] elegible + non pesaje showing in gray | priority:medium
  when elegible is ok but no pesaje, it can also be calibrated, enable the button for those cases.
- [x] sql-databricks-bridge corriendo en servidor | priority:high | tags:deploy,server
- [x] cancel process | priority:medium
  Enable the option to cancel a sync
  Enable the option to cancel a calibration process
- [x] busqueda en dashboard | priority:medium
  /pancha incorporar la opción de buscar/filtrar tareas por nombre en el titulo
- [x] Revisar como correr el server con el ejecutable | priority:high | tags:server,deploy
- [x] Merge kpioe queries to databricks 000 | priority:high | tags:queries,databricks
- [x] Compilar en ejecutable en windows | priority:medium | tags:build,windows
- [x] Jobs with partial failed queries should not be marked as complete | priority:high | tags:frontend,bug,m1
  Should show a warning instead
- [x] Compile front end with tauri in github actions | priority:medium | tags:ci,tauri
- [x] milestone colors have small contrast when on warning | priority:medium
  increase contrast (may be when background is in warning, use dark letters)
- [x] Incorporar query mordom para simulador de pesaje | priority:medium | tags:queries
- [x] Agregar Inicio en stage | priority:medium | tags:frontend,m1
- [x] Validation with Colombia sync | priority:high | tags:frontend,colombia,m1
- [x] Duration always showing 0ms | priority:high | tags:frontend,bug,m1
- [x] Agregar tags a las versiones historicas de Delta tables | priority:medium | tags:feature,delta
- [x] Correr aplicativo para extraer Bolivia | priority:medium | tags:bolivia,extract
- [x] steps and subteps information | priority:medium
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
- [x] Validar que los tags se esten implementando | priority:medium | tags:validation,delta
