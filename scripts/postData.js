function postDb (postElement) {
    $.ajax({
        url: "php/postData.php",
        type: "POST",
        cache: false,
        data: {
            postElement: postElement                        
        },
        success: function(dataResult){
            var dataResult = JSON.parse(dataResult);
            if(dataResult.statusCode==200){
                console.log("Record updated successfully!"); 						
            }
            else if(dataResult.statusCode==201){
               console.log("Error occured! Record was not updated.");
            }
            
        }
    });
}