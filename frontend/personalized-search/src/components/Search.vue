<template>
  <div>
    <v-text-field v-model="query" label="Search" outlined></v-text-field>
    <v-btn @click="search" color="primary">Search</v-btn>
    <v-container>
      <v-row v-if="searchResults.length > 0">
        <v-col cols="12">
          <h3>Search Results:</h3>
          <v-list>
            <v-list-item v-for="result in searchResults" :key="result.id">
              <v-list-item-content>
                <v-list-item-title>{{ result._source.title }}</v-list-item-title>
                <v-list-item-subtitle>{{ result._source.score }}</v-list-item-subtitle>
              </v-list-item-content>
            </v-list-item>
          </v-list>
        </v-col>
      </v-row>
    </v-container>
  </div>
</template>

<script>
export default {
  name: 'SearchView',
  data() {
    return {
      query: '',
      selectedUser: null,
      searchResults: [], // Array to hold search results
      FLASK_SERVER_URL: 'http://localhost:5000'
    };
  },
  methods: {
    // search() {
    //   // Perform search using query and selectedUser
    //   // Send request to backend API endpoint
    //   // Update searchResults with the received search results
    //   respone = getData()
    //   this.searchResults = [
    //     { id: 1, title: 'Search Result 1', description: 'Description of search result 1' },
    //     { id: 2, title: 'Search Result 2', description: 'Description of search result 2' },
    //     { id: 3, title: 'Search Result 3', description: 'Description of search result 3' }
    //   ];
    // },
    async search() {
      try {
        const response = await fetch(`${this.FLASK_SERVER_URL}/api/search`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            index_name: 'document_index', // Change this to your actual index name
            query: this.query
          }),
        });
        const data = await response.json();
        console.log("data hits", data['hits']['hits']); // Log the parsed JSON data
        console.log("data 0", data['hits']['hits'][0]._id); // Log the parsed JSON data
        this.searchResults = data['hits']['hits']; // Assuming the response contains search results directly
      } catch (error) {
        console.error('Error fetching data:', error);
      }
    }
  }
};
</script>
