<template>
  <div>
    <div class="search">
      <v-text-field v-model="query" label="Search" @keyup.enter="onSearch" class="input mx-5 my-2" outlined></v-text-field>
      <v-btn @click="onSearch" color="primary" large class="my-4 px-2">
        <v-icon medium >mdi-magnify</v-icon>
      </v-btn>
      
    </div>
    <div v-if="!selectedResult && selectedResult === null" class="search"> {{ searchResults.length }} Search Results </div>
    <div class="content-center">
      <v-card v-if="!selectedResult && selectedResult === null">
        <v-card-text v-if="msg">
        {{ msg }}
      </v-card-text>
      <v-simple-table v-if="searchResults.length > 0" class="width"> 
        <template v-slot:default>
          <tbody>
            <tr
              v-for="result in searchResults"
              :key="result.id"
            >
              <td @click="showResult(result)" class="clickable content-center pt-3">
                {{ result._source.title }},
                <v-spacer/>
                {{ result._score }}
              </td>
            </tr>
          </tbody>
        </template>
      </v-simple-table>
      </v-card>
      <v-card v-else class="width">
        <v-card-title class="content-center">
          <v-icon @click="selectedResult = null" large class="pr-5" >mdi-arrow-left</v-icon>
          {{ selectedResult._source.title}}
        </v-card-title>
        <v-card-text>
          {{ selectedResult._source.description}}
          <i class="mt-2">{{ selectedResult._source.timestamp}}</i>
        </v-card-text>
      </v-card>
    </div>
  </div>
</template>

<script>
// import {address, index} from '../../../../../backend/config.py'

export default {
  name: 'SearchView',
  props: {
        user: String,
      },
  data() {
    return {
      query: '',
      msg: '',
      selectedResult: null,
      searchResults: [], // Array to hold search results
      FLASK_SERVER_URL: 'http://127.0.0.1:5000',
      INDEX_NAME: 'wiki_index'
    };
  },
  methods: {
    onSearch() {
      if (this.query.trim() !== '') {
        this.msg = ''
        this.search();
      }
      else {
        this.searchResults = []
        this.msg = 'No Search Results'
      }
        this.selectedResult = null
    },
    async search() {
      try {
        const response = await fetch(`${this.FLASK_SERVER_URL}/api/search`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            index_name: this.INDEX_NAME,
            query: this.query
          }),
        });
        const data = await response.json();
        this.searchResults = data['hits']['hits'];
        if (this.searchResults.length < 1) {
          this.msg = 'No Search Results'
        }
      } catch (error) {
        console.error('Error fetching data:', error);
      }
    },
    showResult(result) {
      this.selectedResult = result
      // TO-DO: Store in vuex store for each user
    }
  }
};
</script>

<style>
.search {
  display: flex;
  justify-content: center;
  margin: 20px;
}
.input {
  max-width: 700px;
}
.result {
  display: flex;
  justify-content: center;
  max-width: 800px; /* Adjust the width as needed */
}
.content-center {
  display: flex;
  justify-content: center;
}
.width {
  width: 800px; /* Adjust the width as needed */
}
.clickable {
  cursor: pointer; /* Change cursor to pointer to indicate it's clickable */
  color: blue; /* Change text color */
  text-decoration: underline; /* Add underline to resemble a link */
}
</style>