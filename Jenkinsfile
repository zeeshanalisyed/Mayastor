#!/usr/bin/env groovy

// On-demand E2E infra configuration
// https://mayadata.atlassian.net/wiki/spaces/MS/pages/247332965/Test+infrastructure#On-Demand-E2E-K8S-Clusters

def e2e_build_cluster_job='k8s-build-cluster' // Jenkins job to build cluster
def e2e_destroy_cluster_job='k8s-destroy-cluster' // Jenkins job to destroy cluster
// Environment to run e2e test in (job param of $e2e_build_cluster_job)
def e2e_environment="hcloud-kubeadm"
// Global variable to pass current k8s job between stages
def k8s_job=""

xray_projectkey='MQ'
xray_on_demand_testplan='MQ-1'
xray_nightly_testplan='MQ-17'
xray_continuous_testplan='MQ-33'
xray_test_execution_type='10059'
// do not send xray reports as a result of "bors try"
xray_send_report = (env.BRANCH_NAME == 'trying') ? false : true

// if e2e run does not build its own images, which tag to use when pulling
e2e_continuous_image_tag='v0.8.0'
e2e_reports_dir='artifacts/reports/'

// In the case of multi-branch pipelines, the pipeline
// name a.k.a. job base name, will be the
// 2nd-to-last item of env.JOB_NAME which
// consists of identifiers separated by '/' e.g.
//     first/second/pipeline/branch
// In the case of a non-multibranch pipeline, the pipeline
// name is env.JOB_NAME. This caters for all eventualities.
def getJobBaseName() {
  def jobSections = env.JOB_NAME.tokenize('/') as String[]
  return jobSections.length < 2 ? env.JOB_NAME : jobSections[ jobSections.length - 2 ]
}

// Searches previous builds to find first non aborted one
def getLastNonAbortedBuild(build) {
  if (build == null) {
    return null;
  }

  if(build.result.toString().equals("ABORTED")) {
    return getLastNonAbortedBuild(build.getPreviousBuild());
  } else {
    return build;
  }
}

def isTimed() {
    def causes = currentBuild.getBuildCauses()
    for(cause in causes) {
      if ("${cause}".contains("hudson.triggers.TimerTrigger\$TimerTriggerCause")) {
        return true
      }
    }
    return false
}

def getAliasTag() {
    if (isTimed() == true) {
        return 'nightly'
    }
    return 'ci'
}

def getTag() {
  if (e2e_build_images == true) {
    if (isTimed() == true) {
        return 'nightly'
    }
    def tag = sh(
      // using printf to get rid of trailing newline
      script: "printf \$(git rev-parse --short ${env.GIT_COMMIT})",
      returnStdout: true
    )
    return tag
  } else {
    return e2e_continuous_image_tag
  }
}

def getTestPlan() {
  if (params.e2e_continuous == true)  {
    return xray_continuous_testplan
  }
  def causes = currentBuild.getBuildCauses()
  for(cause in causes) {
    if ("${cause}".contains("hudson.triggers.TimerTrigger\$TimerTriggerCause")) {
      return xray_nightly_testplan
    }
  }
  return xray_on_demand_testplan
}

// Install Loki on the cluster
def lokiInstall(tag) {
  sh 'kubectl apply -f ./mayastor-e2e/loki/promtail_namespace_e2e.yaml'
  sh 'kubectl apply -f ./mayastor-e2e/loki/promtail_rbac_e2e.yaml'
  sh 'kubectl apply -f ./mayastor-e2e/loki/promtail_configmap_e2e.yaml'
  def cmd = "run=\"${env.BUILD_NUMBER}\" version=\"${tag}\" envsubst -no-unset < ./mayastor-e2e/loki/promtail_daemonset_e2e.template.yaml | kubectl apply -f -"
  sh "nix-shell --run '${cmd}'"
}

// Unnstall Loki
def lokiUninstall(tag) {
  def cmd = "run=\"${env.BUILD_NUMBER}\" version=\"${tag}\" envsubst -no-unset < ./mayastor-e2e/loki/promtail_daemonset_e2e.template.yaml | kubectl delete -f -"
  sh "nix-shell --run '${cmd}'"
  sh 'kubectl delete -f ./mayastor-e2e/loki/promtail_configmap_e2e.yaml'
  sh 'kubectl delete -f ./mayastor-e2e/loki/promtail_rbac_e2e.yaml'
  sh 'kubectl delete -f ./mayastor-e2e/loki/promtail_namespace_e2e.yaml'
}

// Send out a slack message if branch got broken or has recovered
def notifySlackUponStateChange(build) {
  def cur = build.getResult()
  def prev = getLastNonAbortedBuild(build.getPreviousBuild())?.getResult()
  if (cur != prev) {
    if (cur == 'SUCCESS') {
      slackSend(
        channel: '#mayastor-backend',
        color: 'normal',
        message: "Branch ${env.BRANCH_NAME} has been fixed :beers: (<${env.BUILD_URL}|Open>)"
      )
    } else if (prev == 'SUCCESS') {
      slackSend(
        channel: '#mayastor-backend',
        color: 'danger',
        message: "Branch ${env.BRANCH_NAME} is broken :face_with_raised_eyebrow: (<${env.BUILD_URL}|Open>)"
      )
    }
  }
}
def notifySlackUponE2EFailure(build) {
  if (build.getResult() != 'SUCCESS' && env.BRANCH_NAME == 'develop') {
    slackSend(
      channel: '#mayastor-e2e',
      color: 'danger',
      message: "E2E continuous testing has failed (<${env.BUILD_URL}|Open>)"
    )
  }
}

// Will ABORT current job for cases when we don't want to build
if (currentBuild.getBuildCauses('jenkins.branch.BranchIndexingCause') &&
    BRANCH_NAME == "develop") {
    print "INFO: Branch Indexing, aborting job."
    currentBuild.result = 'ABORTED'
    return
}

// Only schedule regular builds on develop branch, so we don't need to guard against it
// Run only on one Mayastor pipeline
String cron_schedule = BRANCH_NAME == "develop" && getJobBaseName() == "Mayastor" ? "0 2 * * *" : ""

// Determine which stages to run
if (params.e2e_continuous == true) {
  run_linter = false
  rust_test = false
  grpc_test = false
  moac_test = false
  e2e_test = true
  e2e_test_profile = "continuous"
  // use images from dockerhub tagged with e2e_continuous_image_tag instead of building from current source
  e2e_build_images = false
  // do not push images even when running on master/develop/release branches
  do_not_push_images = true
} else {
  run_linter = false
  rust_test = true
  grpc_test = true
  moac_test = true
  e2e_test = true
  // Some long e2e tests are not suitable to be run for each PR
  e2e_test_profile = (env.BRANCH_NAME != 'staging' && env.BRANCH_NAME != 'trying') ? "extended" : "ondemand"
  e2e_build_images = true
  do_not_push_images = false
}
e2e_alias_tag = getAliasTag()

pipeline {
  agent none
  options {
    timeout(time: 3, unit: 'HOURS')
  }
  parameters {
    booleanParam(defaultValue: false, name: 'e2e_continuous')
  }
  triggers {
    cron(cron_schedule)
  }

  stages {
    stage('init') {
      agent { label 'nixos-mayastor' }
      steps {
        step([
          $class: 'GitHubSetCommitStatusBuilder',
          contextSource: [
            $class: 'ManuallyEnteredCommitContextSource',
            context: 'continuous-integration/jenkins/branch'
          ],
          statusMessage: [ content: 'Pipeline started' ]
        ])
      }
    }
    stage('linter') {
      agent { label 'nixos-mayastor' }
      when {
        beforeAgent true
        not {
          anyOf {
            branch 'master'
            branch 'release/*'
            expression { run_linter == false }
          }
        }
      }
      steps {
        sh 'nix-shell --run "cargo fmt --all -- --check"'
        sh 'nix-shell --run "cargo clippy --all-targets -- -D warnings"'
        sh 'nix-shell --run "./scripts/js-check.sh"'
      }
    }
    stage('test') {
      when {
        beforeAgent true
        not {
          anyOf {
            branch 'master'
            branch 'release/*'
          }
        }
      }
      parallel {
        stage('rust unit tests') {
          when {
            beforeAgent true
            expression { rust_test == true }
          }
          agent { label 'nixos-mayastor' }
          environment {
            START_DATE = new Date().format("yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone('UTC'))
          }
          steps {
            sh 'printenv'
            sh 'nix-shell --run "./scripts/cargo-test.sh"'
          }
          post {
            always {
              // in case of abnormal termination of any nvmf test
              sh 'sudo nvme disconnect-all'
              sh './scripts/check-coredumps.sh --since "${START_DATE}"'
            }
          }
        }
        stage('grpc tests') {
          when {
            beforeAgent true
            expression { grpc_test == true }
          }
          agent { label 'nixos-mayastor' }
          environment {
            START_DATE = new Date().format("yyyy-MM-dd HH:mm:ss", TimeZone.getTimeZone('UTC'))
          }
          steps {
            sh 'printenv'
            sh 'nix-shell --run "./scripts/grpc-test.sh"'
          }
          post {
            always {
              junit '*-xunit-report.xml'
              sh './scripts/check-coredumps.sh --since "${START_DATE}"'
            }
          }
        }
        stage('moac unit tests') {
          when {
            beforeAgent true
            expression { moac_test == true }
          }
          agent { label 'nixos-mayastor' }
          steps {
            sh 'printenv'
            sh 'nix-shell --run "./scripts/moac-test.sh"'
          }
          post {
            always {
              junit 'moac-xunit-report.xml'
            }
          }
        }
        stage('e2e tests') {
          when {
            beforeAgent true
            expression { e2e_test == true }
          }
          stages {
            stage('e2e docker images') {
              when {
                beforeAgent true
                expression { e2e_build_images == true }
              }
              agent { label 'nixos-mayastor' }
              steps {
                // e2e tests are the most demanding step for space on the disk so we
                // test the free space here rather than repeating the same code in all
                // stages.
                sh "./scripts/reclaim-space.sh 10"

                // Build images (REGISTRY is set in jenkin's global configuration).
                // Note: We might want to build and test dev images that have more
                // assertions instead but that complicates e2e tests a bit.
                sh "./scripts/release.sh --alias-tag \"${e2e_alias_tag}\" --registry \"${env.REGISTRY}\""
              }
              post {
                // Always remove all docker images because they are usually used just once
                // and underlaying pkgs are already cached by nix so they can be easily
                // recreated.
                always {
                  sh 'docker image prune --all --force'
                }
              }
            }
            stage('build e2e cluster') {
              agent { label 'nixos' }
              steps {
                script {
                  k8s_job=build(
                    job: "${e2e_build_cluster_job}",
                    propagate: true,
                    wait: true,
                    parameters: [[
                      $class: 'StringParameterValue',
                      name: "ENVIRONMENT",
                      value: "${e2e_environment}"
                    ]]
                  )
                }
              }
            }
            stage('run e2e') {
              agent { label 'nixos' }
              environment {
                KUBECONFIG = "${env.WORKSPACE}/${e2e_environment}/modules/k8s/secrets/admin.conf"
              }
              steps {
                // FIXME(arne-rusek): move hcloud's config to top-level dir in TF scripts
                sh """
                  mkdir -p "${e2e_environment}/modules/k8s/secrets"
                """
                copyArtifacts(
                    projectName: "${k8s_job.getProjectName()}",
                    selector: specific("${k8s_job.getNumber()}"),
                    filter: "${e2e_environment}/modules/k8s/secrets/admin.conf",
                    target: "",
                    fingerprintArtifacts: true
                )
                sh 'kubectl get nodes -o wide'

                script {
                  // get copy of latest mayastor-e2e test files
                  sh 'rm -Rf mayastor-e2e'
                  withCredentials([
                    usernamePassword(credentialsId: 'github-checkout', usernameVariable: 'ghuser', passwordVariable: 'ghpw')
                  ]) {
                    sh "git clone https://${ghuser}:${ghpw}@github.com/mayadata-io/mayastor-e2e.git"
                    sh 'cd mayastor-e2e && git checkout develop'
                  }
                  sh "mkdir -p ./${e2e_reports_dir}"
                  def tag = getTag()
                  def cmd = "./scripts/e2e-test.sh --device /dev/sdb --tag \"${tag}\" --logs --profile \"${e2e_test_profile}\" --build_number \"${env.BUILD_NUMBER}\" --mayastor \"${env.WORKSPACE}\" --reportsdir \"${env.WORKSPACE}/${e2e_reports_dir}\" "
                  // building images also means using the CI registry
                  if (e2e_build_images == true) {
                    cmd = cmd + " --registry \"" + env.REGISTRY + "\""
                  }

                  withCredentials([
                    usernamePassword(credentialsId: 'GRAFANA_API', usernameVariable: 'grafana_api_user', passwordVariable: 'grafana_api_pw')
                  ]) {
                    lokiInstall(tag)
                    sh "nix-shell --run 'cd mayastor-e2e && ${cmd}'"
                    lokiUninstall(tag) // so that, if we keep the cluster, the next Loki instance can use different parameters
                  }
                }
              }
              post {
                failure {
                  script {
                    withCredentials([string(credentialsId: 'HCLOUD_TOKEN', variable: 'HCLOUD_TOKEN')]) {
                      e2e_nodes=sh(
                        script: """
                          nix-shell -p hcloud --run 'hcloud server list' | grep -e '-${k8s_job.getNumber()} ' | awk '{ print \$2" "\$4 }'
                        """,
                        returnStdout: true
                      ).trim()
                    }
                    // Job name for multi-branch is Mayastor/<branch> however
                    // in URL jenkins requires /job/ in between for url to work
                    urlized_job_name=JOB_NAME.replaceAll("/", "/job/")
                    self_url="${JENKINS_URL}job/${urlized_job_name}/${BUILD_NUMBER}"
                    self_name="${JOB_NAME}#${BUILD_NUMBER}"
                    build_cluster_run_url="${JENKINS_URL}job/${k8s_job.getProjectName()}/${k8s_job.getNumber()}"
                    build_cluster_destroy_url="${JENKINS_URL}job/${e2e_destroy_cluster_job}/buildWithParameters?BUILD=${k8s_job.getProjectName()}%23${k8s_job.getNumber()}"
                    kubeconfig_url="${JENKINS_URL}job/${k8s_job.getProjectName()}/${k8s_job.getNumber()}/artifact/hcloud-kubeadm/modules/k8s/secrets/admin.conf"
                    slackSend(
                      channel: '#mayastor-e2e',
                      color: 'danger',
                      message: "E2E k8s cluster <$build_cluster_run_url|#${k8s_job.getNumber()}> left running due to failure of " +
                        "<$self_url|$self_name>. Investigate using <$kubeconfig_url|kubeconfig>, or ssh as root to:\n" +
                        "```$e2e_nodes```\n" +
                        "And then <$build_cluster_destroy_url|destroy> the cluster.\n" +
                        "Note: you need to click `proceed` and will get an empty page when using destroy link. " +
                        "(<https://mayadata.atlassian.net/wiki/spaces/MS/pages/247332965/Test+infrastructure#On-Demand-E2E-K8S-Clusters|doc>)"
                    )
                  }
                }
                always {
                  archiveArtifacts 'artifacts/**/*.*'
                  // TODO get e2e-test.sh to take a configurable dir for artifacts
                  archiveArtifacts 'mayastor-e2e/artifacts/**/*.*'
                  // handle junit results on success or failure
                  junit "${e2e_reports_dir}/*.xml"
                  script {
                    if (xray_send_report == true) {
                      def xray_testplan = getTestPlan()
                      def tag = getTag()
                      step([
                        $class: 'XrayImportBuilder',
                        endpointName: '/junit/multipart',
                        importFilePath: "${e2e_reports_dir}/*.xml",
                        importToSameExecution: 'true',
                        projectKey: "${xray_projectkey}",
                        testPlanKey: "${xray_testplan}",
                        serverInstance: "${env.JIRASERVERUUID}",
                        inputInfoSwitcher: 'fileContent',
                        importInfo: """{
                          "fields": {
                            "summary": "Build #${env.BUILD_NUMBER}, branch: ${env.BRANCH_name}, tag: ${tag}",
                            "project": {
                              "key": "${xray_projectkey}"
                            },
                            "issuetype": {
                              "id": "${xray_test_execution_type}"
                            },
                            "description": "Results for build #${env.BUILD_NUMBER} at ${env.BUILD_URL}"
                          }
                        }"""
                      ])
                    }
                  }
                }
              }
            }
            stage('destroy e2e cluster') {
              agent { label 'nixos' }
              steps {
                script {
                  build(
                    job: "${e2e_destroy_cluster_job}",
                    propagate: true,
                    wait: true,
                    parameters: [
                      [
                        $class: 'RunParameterValue',
                        name: "BUILD",
                        runId:"${k8s_job.getProjectName()}#${k8s_job.getNumber()}"
                      ]
                    ]
                  )
                }
              }
            }
          }
          post {
            success {
              script {
                if (params.e2e_continuous == true && env.E2E_CONTINUOUS_ENABLE == "true") {
                  build job: env.BRANCH_NAME, wait: false, parameters: [[$class: 'BooleanParameterValue', name: 'e2e_continuous', value: true]]
                }
              }
            }
          }
        }// end of "e2e tests" stage
      }// parallel stages block
    }// end of test stage
    stage('push images') {
      agent { label 'nixos-mayastor' }
      when {
        beforeAgent true
        allOf {
          expression { do_not_push_images == false }
          anyOf {
            branch 'master'
            branch 'release/*'
            branch 'develop'
          }
        }
      }
      steps {
        withCredentials([usernamePassword(credentialsId: 'dockerhub', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
          sh 'echo $PASSWORD | docker login -u $USERNAME --password-stdin'
        }
        sh './scripts/release.sh'
      }
      post {
        always {
          sh 'docker logout'
          sh 'docker image prune --all --force'
        }
      }
    }
  }

  // The main motivation for post block is that if all stages were skipped
  // (which happens when running cron job and branch != develop) then we don't
  // want to set commit status in github (jenkins will implicitly set it to
  // success).
  post {
    always {
      node(null) {
        script {
          // If no tests were run then we should neither be updating commit
          // status in github nor send any slack messages
          if (currentBuild.result != null) {
            // Do not update the commit status for continuous tests
            if (params.e2e_continuous == false) {
              step([
                $class: 'GitHubCommitStatusSetter',
                errorHandlers: [[$class: "ChangingBuildStatusErrorHandler", result: "UNSTABLE"]],
                contextSource: [
                  $class: 'ManuallyEnteredCommitContextSource',
                  context: 'continuous-integration/jenkins/branch'
                ],
                statusResultSource: [
                  $class: 'ConditionalStatusResultSource',
                  results: [
                    [$class: 'AnyBuildResult', message: 'Pipeline result', state: currentBuild.getResult()]
                  ]
                ]
              ])
              if (env.BRANCH_NAME == 'develop') {
                notifySlackUponStateChange(currentBuild)
              }
            } else {
              notifySlackUponE2EFailure(currentBuild)
            }
          }
        }
      }
    }
  }
}
