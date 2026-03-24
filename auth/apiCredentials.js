require('dotenv').config();

const USER_CONFIG    = JSON.parse(process.env.USER_CONFIG);

async function getConnectorCredentials() {

      return {
        active: 1,
        apiCredentials: {
        userName:     USER_CONFIG.userName,
        userPassword: USER_CONFIG.userPassword,
        userToken:    USER_CONFIG.userToken
        }
      };

}

module.exports = { 
  getConnectorCredentials, 
};