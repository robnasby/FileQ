module FileQ
  class Queue
    def initialize(directory)
      @directory = directory
    end

    def add(data)
      id = "%10.12f" % Time.now.getutc.to_f

      File.open(filepath_from_id(id), 'w') do |file|
        file.write data
      end

      id
    end

    def release(id)
      filepath = filepath_from_id(id)

      File.open(filepath) do |file|
        file.flock File::LOCK_UN
      end
    end

    def remove(id)
      filepath = filepath_from_id(id)

      if File.exists? filepath
        begin
          File.delete filepath
        rescue Errno::ENOENT
        end
      end
    end

    def retrieve
      item = nil

      Dir.foreach(@directory) do |filename|
        filepath = filepath_from_id filename  
        if !filename.start_with? '.' and File.exists? filepath and !File.directory? filepath
          begin
            file = File.open filepath    
            if file.flock(File::LOCK_NB | File::LOCK_EX)
              data = file.read
              id = filename
              item = QItem.new(data, id)
              break
            end
          rescue Errno::ENOENT
          end
        end    
      end  

      item
    end

  private
    def filepath_from_id(id)
      File.join @directory, id
    end

    def id_from_filepath(filepath)
      File.basename filepath
    end
  end

  class QItem
    attr_reader :data, :id

    def initialize(data, id)
      @data = data
      @id = id
    end
  end
end
