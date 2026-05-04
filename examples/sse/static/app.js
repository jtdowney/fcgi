const log = document.getElementById("log");
const source = new EventSource("/events");

source.addEventListener("tick", (event) => {
  console.log("tick:", event.data);
  const item = document.createElement("li");
  item.textContent = event.data;
  log.appendChild(item);
});

source.addEventListener("error", () => {
  console.log("stream closed");
  source.close();
});
