<template>
  <v-app>
    <v-app-bar
      app
      color="primary"
      dark
    >
      <div class="d-flex align-center">
        <v-icon large class="mx-2" >mdi-search-web</v-icon>
        <b>P-search</b>
      </div>

      <v-spacer></v-spacer>

      <v-btn
        @click="changeUser('user1')"
        text
      >
        <v-icon>mdi-</v-icon>
        <span>User 1</span>
      </v-btn>
      <v-btn
        @click="changeUser('user2')"
        text
      >
        <v-icon>mdi-</v-icon>
        <span class="">User 2</span>
      </v-btn>
      <v-btn
        @click="changeUser('user3')"
        text
      >
        <v-icon>mdi-</v-icon>
        <span class="">User 3</span>
      </v-btn>
    </v-app-bar>

    <v-main class="mt-3">
      <Search @user="user"/>
    </v-main>
  </v-app>
</template>

<script>
import Search from './components/Search.vue';

export default {
  name: 'App',

  components: {
    Search,
  },

  data: () => ({
    user: '',
  }),
  methods: {
    changeUser(otherUser) {
      //TO-DO: save user info
      this.user = otherUser
      console.log("Changed user to", otherUser)
    }
  }
};
</script>
