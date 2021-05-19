pipeline {
    agent { dockerfile true }
    
    environment {
        registry = "mwall2bitflow/logstar-online-stream"
        registryCredential = "hub.docker.com"
    }
    stages {

        stage('Building image') {
            steps{
                script {
                    dockerImage = docker.build registry + ":$BUILD_NUMBER"
                }
            }
        }
        stage('Deploy Image') {
                steps {
                    script {
                        docker.withRegistry( '', registryCredential ) {
                        dockerImage.push()
                    }
                }
            }
        }
        stage('Remove Unused docker image') {
            steps {
                sh "docker rmi $registry:$BUILD_NUMBER"
            }
        }
    }
}

