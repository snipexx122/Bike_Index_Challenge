import requests

class data_ingester:
    
    def __generate_colors_string(self,colors):
        color_string = ""
        colors_fixed = []
        
        if colors !="":
            colors_fixed = colors.split(",")
        
        for i in range(0,len(colors_fixed)):
        
            color_string = color_string + colors_fixed[i]
        
            if i !=len(colors_fixed)-1:
                color_string = color_string + "%2C"
        
        return color_string
    
    def __generate_manufacturer_string(self,manufacturer):
        manufacturer_string = ""
        
        if manufacturer!="":
            manufacturer_string = "manufacturer = "+manufacturer
        
        return manufacturer_string

    def __generate_year_string(self,year):
        year_string = ""
        
        if year !="":
            year_string = "query=year%20%3D%20"+year
        
        return year_string
    
    def __generate_stolness_string(self,stoleness):
        stoleness_string = "stolenness=all"
        
        if stoleness != "":
            stoleness_string = "stolenness="+stoleness
        
        return stoleness_string

    def __data_engine(self,url):
        try : 
            response = requests.get(url)
            
            if response.status_code == 200:
                print(str(response.status_code) + " connected! data being ingested")
                data = response.json()
                return data
            
            else:
                raise Exception("API problem in getting data")
        
        except:
            raise Exception("Connection problem or Connection string problem")
    
    def __get_data_from_API(self,page,year,manufacturer,colors,stoleness):
        color_string = self.__generate_colors_string(colors)
        
        manufacturer_string = self.__generate_manufacturer_string(manufacturer)
        
        year_string = self.__generate_year_string(year)

        stoleness_string = self.__generate_stolness_string(stoleness)
        
        url = "https://bikeindex.org/api/v3/search?page={}&per_page=25&location=IP&{}&{}&{}&{}".format(str(page),color_string,manufacturer_string,year_string,stoleness_string)

        data = self.__data_engine(url)
        
        return data
        
    def __parse_data(self,data):

        return data

    def generate_data(self,page,year,manufacturer,colors,stoleness):
    
        data = self.__get_data_from_API(page,year,manufacturer,colors,stoleness)

        data = self.__parse_data(data)
        return data