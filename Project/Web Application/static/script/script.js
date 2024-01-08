//This function fetches data from a specified URL using the Fetch API, waits for the response, parses it as JSON, and returns the data.
async function fetchData(url) {
    const response = await fetch(url);
    const data = await response.json();
    return data;
}
//This function gets JSON data from a URL, 
//calculates the total tweet count from the data, 
//and returns the count with "Tweets" or "Zero Tweets" if there is no data.
async function fetchTotalTweets(url) {
        const response = await fetch(url);
        const data = await response.json();
        let totalTweets = 0;
        if (Array.isArray(data) && data.length > 0) {
            for (const entry of data) {
                totalTweets += entry.count;
            }
            return totalTweets + " Tweets";
        }
        else return  " Zero Tweets";
}
//This function calls two other functions, getTopUsers and getTweetDistribution, to update data on the page.
function updateData() {
    getTopUsers();
    getTweetDistribution()
}
//This function fetches data about top users, updates an HTML table, and creates a pie chart to visualize their tweet counts.
async function getTopUsers() {
    const topUsersData = await fetchData("/top_users");
    const topUsersBody = document.getElementById("topUsersBody");
    topUsersBody.innerHTML = "";
    topUsersData.forEach((user) => {
        const row = document.createElement("tr");
        row.innerHTML = `<td>${user._id}</td><td>${user.count}</td>`;
        topUsersBody.appendChild(row);
    });
    const userNames = topUsersData.map((user) => user._id);
    const tweetCounts = topUsersData.map((user) => user.count);
    const ctx = document.getElementById("topUsersChart").getContext("2d");
    new Chart(ctx, {
        type: "pie",
        data: {
            labels: userNames,
            datasets: [
                {
                    data: tweetCounts,
                    backgroundColor: [
                        "rgba(45, 156, 73, 0.8)",
                        "rgba(54, 162, 235, 0.8)",
                        "rgba(255, 206, 86, 0.8)",
                        "rgba(75, 192, 192, 0.8)",
                        "rgba(153, 102, 255, 0.8)",
                        "rgba(255, 159, 64, 0.8)",
                    ],
                },
            ],
        },
    });
}

//This function fetches tweet distribution data for a specified username,
// updates the HTML to display the total tweet count, 
//and creates or updates a line chart to visualize tweet counts over time.
async function getTweetDistribution() {
    const username = document.getElementById("username").value;
    const distributionData = await fetchData(`/tweet_distribution/${username}`);
    const totalTweets = await fetchTotalTweets(`/tweet_distribution/${username}`);
    const tweetDistributionDiv = document.getElementById("tweetDistribution");
    tweetDistributionDiv.innerHTML = `Tweet Count: ${totalTweets}`;
    const dates = distributionData.map((entry) => entry._id);
    const tweetCounts = distributionData.map((entry) => entry.count);
    const ctx = document.getElementById("tweetDistributionChart").getContext("2d");
    if (window.myChart) {
        window.myChart.destroy();
    }
    window.myChart = new Chart(ctx, {
        type: "line",
        data: {
            labels: dates,
            datasets: [
                {
                    label: "Tweets Over Time",
                    borderColor: "rgba(76, 175, 81, 0.8)",
                    data: tweetCounts,
                },
            ],
        },
    });
}

//This refreshes the displayed data by running the updateData function every 5 seconds.
setInterval(updateData, 5000);
//This triggers the getTopUsers function when the webpage finishes loading.
document.addEventListener("DOMContentLoaded", getTopUsers);