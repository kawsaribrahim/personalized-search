<template>
  <v-app>
    <v-app-bar
      app
      color="primary"
      dark
    >
      <div class="d-flex align-center">
        <v-img
          alt="Vuetify Logo"
          class="shrink mr-2"
          contain
          src="https://cdn.vuetifyjs.com/images/logos/vuetify-logo-dark.png"
          transition="scale-transition"
          width="40"
        />

        <v-img
          alt="Vuetify Name"
          class="shrink mt-1 hidden-sm-and-down"
          contain
          min-width="100"
          src="https://cdn.vuetifyjs.com/images/logos/vuetify-name-dark.png"
          width="100"
        />
      </div>

      <v-btn
        @click="changeUser('user1')"
        text
      >
        <v-icon>mdi-</v-icon>
        <span class="ml-2">User 1</span>
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

      <v-spacer></v-spacer>

      <v-btn
        href="https://github.com/vuetifyjs/vuetify/releases/latest"
        target="_blank"
        text
      >
        <span class="mr-2">Latest Release</span>
        <v-icon>mdi-open-in-new</v-icon>
      </v-btn>
    </v-app-bar>

    <v-main>
      <Search/>
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
    //
  }),
  methods: {
    changeUser(user) {
      //TO-DO: save user info
      console.log("Changed user to", user)
    }
  }
};
</script>
