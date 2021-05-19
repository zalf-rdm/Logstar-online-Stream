pipeline {
    agent { dockerfile true }
    
    environment {
        version = 0.1
        registry = "mwall2bitflow/logstar-online-stream"
        registryCredential = "hub.docker.com"
    }
    stages {
        stage('prepare  ') {
            steps{
                
                sh 'apt-get -y install apt-transport-https ca-certificates curl gnupg lsb-release'
                sh 'curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg'
                sh 'echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
                    $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null'
                sh 'apt-get update && apt-get -y install docker-ce docker-ce-cli containerd.io'
            }
        }
        stage('Building image') {
            steps{
                script {
                    dockerImage = docker.build registry + ":$BUILD_NUMBER"
                }
            }
        }
        stage('Deploy Image') {
            when {
                 branch 'master'
            }
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
                sh "docker rmi $registry:$(version).$BUILD_NUMBER"
            }
        }
    }
}

